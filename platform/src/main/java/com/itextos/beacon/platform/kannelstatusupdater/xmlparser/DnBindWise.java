package com.itextos.beacon.platform.kannelstatusupdater.xmlparser;

import jakarta.xml.bind.annotation.XmlElement;
import jakarta.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "dlr")
public class DnBindWise {

    @XmlElement(name = "received")
    public String dn;
}