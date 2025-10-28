package com.itextos.beacon.platform.kannelstatusupdater.xmlparser;

import jakarta.xml.bind.annotation.XmlElement;
import jakarta.xml.bind.annotation.XmlElementRef;
import jakarta.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "sms")
public class Sms {

    @XmlElement(name = "storesize")
    public String storesize;

    @XmlElement(name = "inbound")
    public String inbound;

    @XmlElement(name = "outbound")
    public String outbound;

    @XmlElementRef
    public SmsSent smssent;
}