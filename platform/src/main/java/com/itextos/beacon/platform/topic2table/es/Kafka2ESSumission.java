package com.itextos.beacon.platform.topic2table.es;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.apache.http.HttpHost;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.json.simple.JSONObject;

import com.itextos.beacon.commonlib.message.BaseMessage;
import com.itextos.beacon.commonlib.message.IMessage;
import com.itextos.beacon.commonlib.utility.CommonUtility;
import com.itextos.beacon.errorlog.K2ESDataLog;
import com.itextos.beacon.errorlog.K2ESLog;

public class Kafka2ESSumission
      
{

    private static final K2ESLog                              log                     = K2ESLog.getInstance();
    private static final K2ESDataLog                              logdata                     = K2ESDataLog.getInstance();

    private final AtomicBoolean             stopped         = new AtomicBoolean(false);

    private KafkaConsumer<String, IMessage> TopicConsumer   = null;

    public String                           ConsumerThreadName;
    private final String                    ConsumerMode;
    private final AppConfiguration          AppConfig;
    private final String                    ESIndexName;
    private final String                    ESIndexUniqueColumn;
    public final String                     ESFmsgIndexName;
    public final String                     ESFmsgIndexUniqueColumn;

    private final int                       ESRetryConflictCount;

    private BulkRequest                     bulkRequest     = null;
    private BulkRequest                     fmsgBulkRequest = null;

    private final int                       IdleFlushTime;
    private final int                       FlushLimit;
    private final int                       LogProcLimit;

    private long                            IdleTimeMS      = 0L;
    private int                             BatchCount      = 0;
    private int                             ProcCount       = 0;
    private int                             LogProcCount    = 0;

    public Kafka2ESSumission(
            String pThreadName,String tpoicgroupname,String topicname)
    {
        this.ConsumerMode            = SubmissionK2ES.AppMode;
        this.AppConfig               = SubmissionK2ES.AppConfig;
        this.ESIndexName             = SubmissionK2ES.ESIndexName;
        this.ESIndexUniqueColumn     = SubmissionK2ES.ESIndexUniqueColumn;
        this.ESFmsgIndexName         = SubmissionK2ES.ESFmsgIndexName;
        this.ESFmsgIndexUniqueColumn = SubmissionK2ES.ESFmsgIndexUniqueColumn;
        this.ESRetryConflictCount    = this.AppConfig.getInt("es.update.retry.count");
        this.FlushLimit              = this.AppConfig.getInt("es.index.flush.limit");
        this.IdleFlushTime           = this.AppConfig.getInt("consumer.idle.flushtime.ms");
        this.LogProcLimit            = this.AppConfig.getInt("consumer.log.proc.limit");

    }

    RestHighLevelClient esConnect()
    {
        final String         ESHosts         = this.AppConfig.getString("es.servers");
        final String[]       ESHSplit        = ESHosts.split("[,]");
        final int            ESConnecTimeOut = this.AppConfig.getInt("es.connection.timeout");
        final int            ESSocketTimeout = this.AppConfig.getInt("es.socket.timeout");

        final List<HttpHost> httpHosts       = Arrays.stream(ESHSplit)
                .map(s -> s.split("[:]"))
                .map(strings -> new HttpHost(strings[0], Integer.valueOf(strings[1])))
                .collect(Collectors.toList());

        log.info("Connecting to ElasticSearch Nodes: " + httpHosts.toString());

        final RestClientBuilder   builder    = RestClient.builder(
                httpHosts.toArray(new HttpHost[httpHosts.size()]))
                .setRequestConfigCallback(
                        requestConfigBuilder -> requestConfigBuilder
                                .setConnectTimeout(ESConnecTimeOut)
                                .setSocketTimeout(ESSocketTimeout));
        final RestHighLevelClient restClient = new RestHighLevelClient(builder);

        return restClient;
    }

    

    

    private void writeData()
            throws Exception
    {
        log.debug("Kafka2ESSumission  writeData() ");

        RestHighLevelClient             ESClient        = esConnect();


     
        // ESClient.bulk(bulkRequest, RequestOptions.DEFAULT);
        log.debug(" Kafka2ESSumission start  bulkRequest size "+bulkRequest.requests().size() );
        final ESBulkAsyncListener bal = new ESBulkAsyncListener(bulkRequest, false);
        ESClient.bulkAsync(bulkRequest, RequestOptions.DEFAULT, bal);
        bulkRequest = new BulkRequest();
        log.debug(" Kafka2ESSumission end  bulkRequest size "+bulkRequest.requests().size() );

        if (fmsgBulkRequest.requests().size() > 0)
        {
            // ESClient.bulk(bulkRequest, RequestOptions.DEFAULT);
            log.debug(" Kafka2ESSumission start  fmsgBulkRequest size "+fmsgBulkRequest.requests().size() );

            final ESBulkAsyncListener balFmsg = new ESBulkAsyncListener(fmsgBulkRequest, true);
            ESClient.bulkAsync(fmsgBulkRequest, RequestOptions.DEFAULT, balFmsg);
            fmsgBulkRequest = new BulkRequest();
            log.debug(" Kafka2ESSumission end  fmsgBulkRequest size "+fmsgBulkRequest.requests().size() );

        }

        ESClient.close();

    }
 
    public void pushtoElasticSearch( List<BaseMessage> mMessagesToInsert) {
    	
        log.info("Kafka2ESSumission pushtoElasticSearch: ConsumerMode :" +this.ConsumerMode);

        for (final BaseMessage data : mMessagesToInsert)
        {
            JSONObject     dataJSON = null;

            try
            {
                if (this.ConsumerMode.equals(Kafka2ESConstants.subMode))
                    dataJSON = Kafka2ESJSONUtilSubmission.buildSubJSON(data);
                else
                    if (this.ConsumerMode.equals(Kafka2ESConstants.delMode))
                        dataJSON = Kafka2ESJSONUtilDeliveries.buildDelJSON(data);
            }
            catch (final Exception ex)
            {
                log.error("Error while processing Message Object", ex);
                ex.printStackTrace(System.err);
                if (log.isDebugEnabled())
                    log.debug(data.toString());
            }

            if (dataJSON == null)
            {
                log.error("Unable to build JSON Object from Message Object");

                if (log.isDebugEnabled())
                    log.debug(data.toString());

                continue;
            }

            log.debug("Kafka2ESSumission dataJSON : "+dataJSON);

            final String        msgId         = CommonUtility.nullCheck(dataJSON.get(this.ESIndexUniqueColumn), true);
            final UpdateRequest updateRequest = new UpdateRequest(this.ESIndexName, msgId)
                    .doc(dataJSON.toJSONString(), XContentType.JSON)
                    .docAsUpsert(true);
            updateRequest.retryOnConflict(ESRetryConflictCount);
            bulkRequest.add(updateRequest);
            log.debug("Kafka2ESSumission after add bulkRequest : msgId "+msgId);

            final String baseMsgId = CommonUtility.nullCheck(dataJSON.get(this.ESFmsgIndexUniqueColumn), true);

            if (!"".equals(baseMsgId))
            {
                JSONObject fmsgJSON = null;
                if (this.ConsumerMode.equals(Kafka2ESConstants.subMode))
                    fmsgJSON = Kafka2ESJSONUtilSubmission.buildSubFMSGJSON(dataJSON, baseMsgId);
                else
                    if (this.ConsumerMode.equals(Kafka2ESConstants.delMode))
                        fmsgJSON = Kafka2ESJSONUtilDeliveries.buildDelFMSGJSON(dataJSON, baseMsgId);

                if (fmsgJSON != null)
                {
                    final UpdateRequest fmsgupdateRequest = new UpdateRequest(this.ESFmsgIndexName, baseMsgId)
                            .doc(fmsgJSON.toJSONString(), XContentType.JSON)
                            .docAsUpsert(true);
                    fmsgupdateRequest.retryOnConflict(ESRetryConflictCount);
                    fmsgBulkRequest.add(fmsgupdateRequest);
                    
                    log.debug("Kafka2ESSumission after add fmsgBulkRequest : baseMsgId "+baseMsgId);

                }
            }

            log.debug("Kafka2ESSumission  single record processs end ");

           
        }
        
        try {
        	
            log.debug("Kafka2ESSumission  writeData ");

			writeData();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    
    }

}
