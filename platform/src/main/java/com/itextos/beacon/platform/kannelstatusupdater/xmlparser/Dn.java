package com.itextos.beacon.platform.kannelstatusupdater.xmlparser;

import jakarta.xml.bind.annotation.XmlElement;
import jakarta.xml.bind.annotation.XmlElementRef;
import jakarta.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "dlr")
public class Dn {

    @XmlElementRef
    public DnReceived dnreceived;

    @XmlElement(name = "inbound")
    public String inbound;

    @XmlElement(name = "queued")
    public String queued;
}