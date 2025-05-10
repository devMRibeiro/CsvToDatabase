package com.github.devmribeiro.csvfile.log;

import com.github.devmribeiro.zenlog.impl.Logger;

public class Log {

	private static final Logger log = Logger.getLogger(Log.class);
	
	public static void t(Object msg) {
    	log.t(msg);
    }

    public static void d(Object msg) {
    	log.d(msg);
    }

    public static void i(Object msg) {
    	log.i(msg);
    }

    public static void w(Object msg) {
    	log.w(msg);
    }

    public static void e(Object msg) {
    	log.e(msg);
	}

    public static void e(Object msg, Throwable t) { 
    	log.e(msg, t);
	}

    public static void f(Object msg) {
    	log.f(msg);
	}
}