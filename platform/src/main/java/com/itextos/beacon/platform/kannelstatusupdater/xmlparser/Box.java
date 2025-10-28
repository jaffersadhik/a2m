package com.itextos.beacon.platform.kannelstatusupdater.xmlparser;

import jakarta.xml.bind.annotation.XmlElement;
import jakarta.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "box")
public class Box {

    @XmlElement(name = "queue")
    public String queue;

    @XmlElement(name = "type")
    public String type;
}