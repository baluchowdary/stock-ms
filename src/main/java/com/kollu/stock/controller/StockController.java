package com.kollu.stock.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kollu.stock.dto.CustomerOrder;
import com.kollu.stock.dto.DeliveryEvent;
import com.kollu.stock.dto.PaymentEvent;
import com.kollu.stock.dto.Stock;
import com.kollu.stock.entity.StockRepository;
import com.kollu.stock.entity.WareHouse;

import lombok.extern.slf4j.Slf4j;

@RestController
@RequestMapping("/api")
@Slf4j
public class StockController {

	@Autowired
	private StockRepository repository;

	@Autowired
	private KafkaTemplate<String, DeliveryEvent> kafkaTemplate;

	@Autowired
	private KafkaTemplate<String, PaymentEvent> kafkaPaymentTemplate;
	
	private static final String NEW_PAYMENTS_TOPIC = "new-payments";
	private static final String NEW_PAYMENTS_GROUP = "payments-group";
	private static final String NEW_STOCK_TOPIC = "new-stock";
	private static final String REVERSED_PAYMENTS_TOPIC = "reversed-payments";
	
	
	@KafkaListener(topics = NEW_PAYMENTS_TOPIC, groupId = NEW_PAYMENTS_GROUP)
	public void updateStock(String paymentEvent) throws JsonMappingException, JsonProcessingException {
		//System.out.println("Inside update inventory for order "+paymentEvent);
		
		log.info("updateStock -- method start");
		
		DeliveryEvent event = new DeliveryEvent();

		PaymentEvent p = new ObjectMapper().readValue(paymentEvent, PaymentEvent.class);
		CustomerOrder order = p.getOrder();

		try {
			Iterable<WareHouse> inventories = repository.findByItem(order.getItem());

			boolean exists = inventories.iterator().hasNext();

			if (!exists) {
				System.out.println("Stock not exist so reverting the order");
				throw new Exception("Stock not available");
			}

			inventories.forEach(i -> {
				i.setQuantity(i.getQuantity() - order.getQuantity());

				repository.save(i);
			});

			event.setType("STOCK_UPDATED");
			event.setOrder(p.getOrder());
			kafkaTemplate.send(NEW_STOCK_TOPIC, event);
			log.info("updateStock -- method end");
			
		} catch (Exception e) {
			PaymentEvent pe = new PaymentEvent();
			pe.setOrder(order);
			pe.setType("PAYMENT_REVERSED");
			kafkaPaymentTemplate.send(REVERSED_PAYMENTS_TOPIC, pe);
		}
	}

	@PostMapping("/addItems")
	public void addItems(@RequestBody Stock stock) {
		Iterable<WareHouse> items = repository.findByItem(stock.getItem());

		if (items.iterator().hasNext()) {
			items.forEach(i -> {
				i.setQuantity(stock.getQuantity() + i.getQuantity());
				repository.save(i);
			});
		} else {
			WareHouse i = new WareHouse();
			i.setItem(stock.getItem());
			i.setQuantity(stock.getQuantity());
			repository.save(i);
		}
	}
}
