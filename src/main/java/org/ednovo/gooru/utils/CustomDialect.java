package org.ednovo.gooru.utils;

import java.sql.Types;

import org.hibernate.Hibernate;
import org.hibernate.dialect.MySQLDialect;

public class CustomDialect extends MySQLDialect{
	
	public CustomDialect(){
		registerHibernateType(Types.LONGVARCHAR, Hibernate.TEXT.getName());
		registerHibernateType(Types.TINYINT, Hibernate.INTEGER.getName());
		registerHibernateType(Types.VARCHAR, Hibernate.TEXT.getName());
	}

}
