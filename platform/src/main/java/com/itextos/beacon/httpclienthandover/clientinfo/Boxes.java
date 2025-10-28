package com.itextos.beacon.httpclienthandover.clientinfo;

import com.itextos.beacon.platform.kannelstatusupdater.xmlparser.Box;

import jakarta.xml.bind.annotation.XmlElementRef;
import jakarta.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "boxes")
public class Boxes {

    @XmlElementRef
    public Box box;
}