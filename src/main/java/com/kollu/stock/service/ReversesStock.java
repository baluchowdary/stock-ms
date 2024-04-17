package com.kollu.stock.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kollu.stock.dto.DeliveryEvent;
import com.kollu.stock.dto.PaymentEvent;
import com.kollu.stock.entity.StockRepository;
import com.kollu.stock.entity.WareHouse;

@RestController
public class ReversesStock {

	@Autowired
	private StockRepository repository;

	@Autowired
	private KafkaTemplate<String, PaymentEvent> kafkaTemplate;
	
	private static final String REVERSED_STOCK_TOPIC = "reversed-stock";
	private static final String REVERSED_STOCK_GROUP = "stock-group";
	private static final String REVERSED_PAYMENTS_TOPIC = "reversed-payments";
	

	@KafkaListener(topics = REVERSED_STOCK_TOPIC, groupId = REVERSED_STOCK_GROUP)
	public void reverseStock(String event) {
		System.out.println("Inside reverse stock for order "+event);
		
		try {
			DeliveryEvent deliveryEvent = new ObjectMapper().readValue(event, DeliveryEvent.class);

			Iterable<WareHouse> inv = this.repository.findByItem(deliveryEvent.getOrder().getItem());

			inv.forEach(i -> {
				i.setQuantity(i.getQuantity() + deliveryEvent.getOrder().getQuantity());
				repository.save(i);
			});

			PaymentEvent paymentEvent = new PaymentEvent();
			paymentEvent.setOrder(deliveryEvent.getOrder());
			paymentEvent.setType("PAYMENT_REVERSED");
			kafkaTemplate.send(REVERSED_PAYMENTS_TOPIC, paymentEvent);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
