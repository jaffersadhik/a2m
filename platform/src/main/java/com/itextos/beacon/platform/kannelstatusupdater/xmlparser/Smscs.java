package com.itextos.beacon.platform.kannelstatusupdater.xmlparser;

import java.util.List;

import jakarta.xml.bind.annotation.XmlElement;
import jakarta.xml.bind.annotation.XmlElementRef;
import jakarta.xml.bind.annotation.XmlRootElement;

@XmlRootElement(
        name = "smscs")
public class Smscs
{

    @XmlElement(
            name = "count")
    public String count;

    @XmlElementRef
    List<Smsc>    smsclist;

}