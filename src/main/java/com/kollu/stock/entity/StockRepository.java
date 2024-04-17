package com.kollu.stock.entity;

import org.springframework.data.jpa.repository.JpaRepository;

public interface StockRepository extends JpaRepository<WareHouse, Long> {

	Iterable<WareHouse> findByItem(String item);

}
