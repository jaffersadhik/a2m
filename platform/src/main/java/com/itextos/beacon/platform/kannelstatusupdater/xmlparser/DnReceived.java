package com.itextos.beacon.platform.kannelstatusupdater.xmlparser;

import jakarta.xml.bind.annotation.XmlElement;
import jakarta.xml.bind.annotation.XmlRootElement;

@XmlRootElement(
        name = "received")
public class DnReceived
{

    @XmlElement(
            name = "total")
    public String total;

}