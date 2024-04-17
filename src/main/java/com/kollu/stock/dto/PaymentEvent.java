package com.kollu.stock.dto;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class PaymentEvent {
	
	private String type;

	private CustomerOrder order;

}
