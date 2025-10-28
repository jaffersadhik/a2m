package com.itextos.beacon.platform.kannelstatusupdater.xmlparser;

import jakarta.xml.bind.annotation.XmlElement;
import jakarta.xml.bind.annotation.XmlRootElement;

@XmlRootElement(
        name = "sms")
public class SmsBindWise
{

    @XmlElement(
            name = "sent")
    public String sms;

}