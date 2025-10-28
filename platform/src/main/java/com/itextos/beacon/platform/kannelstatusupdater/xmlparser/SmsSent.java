package com.itextos.beacon.platform.kannelstatusupdater.xmlparser;

import jakarta.xml.bind.annotation.XmlElement;
import jakarta.xml.bind.annotation.XmlRootElement;

@XmlRootElement(
        name = "sent")
public class SmsSent
{

    @XmlElement(
            name = "total")
    public String total;

    @XmlElement(
            name = "queued")
    public String queued;

}