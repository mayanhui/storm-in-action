package chapter04.web.service;

import java.util.List;

import chapter04.web.bean.StormBean;
import chapter04.web.jdbc.Stormjdbc;


public class StormService {
	
	 public List<StormBean> findAll(){
			return Stormjdbc.findAll();
	 }
}
