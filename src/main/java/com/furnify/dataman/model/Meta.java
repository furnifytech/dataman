package com.furnify.dataman.model;

import lombok.Data;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;

@Data
@XmlAccessorType(XmlAccessType.FIELD)
public class Meta {
	@XmlElement(name = "pk")
	private String pk;
	
	@XmlElement(name = "cols")
	private String cols;
}
