package chapter04.web.service;

import java.util.List;

import chapter04.web.bean.LatLngBean;
import chapter04.web.jdbc.Stormjdbc;


public class StormService {
	 public List<LatLngBean> findAll(){
			return Stormjdbc.findAll();
	 }
}
