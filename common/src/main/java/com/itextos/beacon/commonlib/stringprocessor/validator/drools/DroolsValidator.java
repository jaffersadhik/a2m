package com.itextos.beacon.commonlib.stringprocessor.validator.drools;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.kie.api.KieBase;
import org.kie.api.KieServices;
import org.kie.api.builder.KieBuilder;
import org.kie.api.builder.KieFileSystem;
import org.kie.api.builder.KieRepository;
import org.kie.api.builder.Message;
import org.kie.api.builder.Results;
import org.kie.api.io.Resource;
import org.kie.api.io.ResourceType;
import org.kie.api.runtime.KieContainer;
import org.kie.api.runtime.KieSession;

public class DroolsValidator
{
    private static final Log log = LogFactory.getLog(DroolsValidator.class);

    private static class SingletonHolder
    {
        static final DroolsValidator INSTANCE = new DroolsValidator();
    }

    public static DroolsValidator getInstance()
    {
        return SingletonHolder.INSTANCE;
    }

    private final Map<String, KieBase> kieBaseMap = new HashMap<>();
    private final KieServices kieServices = KieServices.Factory.get();

    public boolean validate(String aFilePath, String aValue)
    {
        final KieBase kieBase = kieBaseMap.computeIfAbsent(aFilePath, k -> getKieBase(aFilePath));

        if (kieBase == null)
            return false;

        final Response response = new Response(aValue);
        createSession(kieBase, response);
        return response.isValidated();
    }

    private static void createSession(KieBase aKieBase, Response aResponse)
    {
        final KieSession ksession = aKieBase.newKieSession();

        ksession.insert(aResponse);
        ksession.fireAllRules();
        ksession.dispose();
    }

    private KieBase getKieBase(String aFilePath)
    {
        try
        {
            return readKieBase(aFilePath);
        }
        catch (final Exception e)
        {
            log.error("Exception while reading drools knowledge base | Drools filePath " + aFilePath, e);
        }
        return null;
    }

    private KieBase readKieBase(String aFilePath)
    {
        // Read the DRL file as a resource
        Resource resource = kieServices.getResources().newFileSystemResource(aFilePath)
                .setResourceType(ResourceType.DRL);

        // Create KieFileSystem and add resource
        KieFileSystem kfs = kieServices.newKieFileSystem();
        kfs.write("src/main/resources/rules.drl", resource);

        // Build the KieModule
        KieBuilder kieBuilder = kieServices.newKieBuilder(kfs).buildAll();

        Results results = kieBuilder.getResults();
        if (results.hasMessages(Message.Level.ERROR))
        {
            for (Message message : results.getMessages(Message.Level.ERROR))
            {
                log.error("Drools file : '" + aFilePath + "' Error : '" + message.getText() + "'");
            }
            throw new IllegalArgumentException("Could not parse knowledge.");
        }

        // Get KieContainer and KieBase
        KieRepository kieRepository = kieServices.getRepository();
        KieContainer kieContainer = kieServices.newKieContainer(kieRepository.getDefaultReleaseId());
        return kieContainer.getKieBase();
    }
}