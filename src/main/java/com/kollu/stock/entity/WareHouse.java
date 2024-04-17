package com.kollu.stock.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@Entity(name = "WareHouse_Table")
public class WareHouse {

	@Id
	@GeneratedValue
	private long id;

	@Column(name = "WareHouse_Quantity")
	private int quantity;

	@Column(name = "WareHouse_Item")
	private String item;
}
