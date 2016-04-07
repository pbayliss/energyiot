package com.datastax.meters.webservice;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;

import javax.jws.WebService;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.meters.model.Transaction;
import com.datastax.meters.service.SearchService;
import com.datastax.meters.service.SearchServiceImpl;

@WebService
@Path("/")
public class MetersWS {

	private Logger logger = LoggerFactory.getLogger(MetersWS.class);
	private SimpleDateFormat inputDateFormat = new SimpleDateFormat("yyyyMMdd");

	//Service Layer.
	private SearchService service = new SearchServiceImpl();
	
	@GET
	@Path("/gettransactions/{deviceid}/{from}/{to}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getMovements(@PathParam("deviceid") String deviceID, @PathParam("from") String fromDate,
			@PathParam("to") String toDate) {
		
		DateTime from = DateTime.now();
		DateTime to = DateTime.now();
		try {
			from = new DateTime(inputDateFormat.parse(fromDate));
			to = new DateTime(inputDateFormat.parse(toDate));
		} catch (ParseException e) {
			String error = "Caught exception parsing dates " + fromDate + "-" + toDate;
			
			logger.error(error);
			return Response.status(Status.BAD_REQUEST).entity(error).build();
		}
				
		List<Transaction> result = service.getTransactionsByTagAndDate(deviceID, from, to);
                
		
		return Response.status(Status.OK).entity(result).build();
	}
	
}
