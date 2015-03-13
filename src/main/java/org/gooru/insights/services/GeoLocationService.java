package org.gooru.insights.services;

import java.io.File;
import java.io.IOException;

import org.gooru.insights.models.ServerLocation;
import org.springframework.stereotype.Service;

import com.maxmind.geoip.Location;
import com.maxmind.geoip.LookupService;
import com.maxmind.geoip.regionName;
 
@Service
public class GeoLocationService {
	
	public ServerLocation getLocation(String ipAddress) {

		ClassLoader classLoader = getClass().getClassLoader();
		File file = new File(classLoader.getResource("GeoLiteCity.dat").getFile());
		return getLocation(ipAddress, file);

	}
 
	public ServerLocation getLocation(String ipAddress, File file) {

		ServerLocation serverLocation = null;

		try {

			serverLocation = new ServerLocation();
			LookupService lookup = new LookupService(file, LookupService.GEOIP_MEMORY_CACHE);
			Location locationServices = lookup.getLocation(ipAddress);
			serverLocation.setCountryCode(locationServices.countryCode);
			serverLocation.setCountryName(locationServices.countryName);
			serverLocation.setRegion(locationServices.region);
			serverLocation.setRegionName(regionName.regionNameByCode(locationServices.countryCode, locationServices.region));
			serverLocation.setCity(locationServices.city);
			serverLocation.setPostalCode(locationServices.postalCode);
			serverLocation.setLatitude(String.valueOf(locationServices.latitude));
			serverLocation.setLongitude(String.valueOf(locationServices.longitude));

		} catch (IOException e) {
			System.err.println(e.getMessage());
		}

		return serverLocation;

	}
}

