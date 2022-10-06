package edu.yu.cs.com3800;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public interface LoggingServer {
    default Logger initializeLogging(String name, boolean bool) {
        Logger logger = Logger.getLogger(name);
        // sets whether logger should also log to terminal
        logger.setUseParentHandlers(bool);
        logger.setLevel(Level.ALL);
        logger = fileHandler(logger, name);
        logger = consoleHandler(logger);
        return logger;
    }

    default Logger initializeLogging(String name) {
        Logger logger = Logger.getLogger(name);
        logger.setUseParentHandlers(false);
        logger.setLevel(Level.ALL);
        logger = fileHandler(logger, name);
        logger = consoleHandler(logger);
        return logger;
    }

    private Logger consoleHandler(Logger logger){
        /*ConsoleHandler ch = new ConsoleHandler();
        ch.setLevel(Level.SEVERE);
        logger.addHandler(ch);*/
        return logger;
    }

    private Logger fileHandler(Logger logger, String name) {
        try {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd-kk_mm");
            new File("./logs/").mkdirs();
            String filePath = "./logs/" + sdf.format(new Date()) + name + ".log";
            FileHandler fh = new FileHandler(filePath);
            if(filePath.contains("HeartbeatMessageLog")){
                File file = new File("GossipMessagePaths.txt");
                file.createNewFile();
                FileOutputStream fos = new FileOutputStream(file,true);
                fos.write((filePath + "\n").getBytes());
                fos.close();
            }
            logger.addHandler(fh);
            SimpleFormatter formatter = new SimpleFormatter();
            fh.setFormatter(formatter);
            logger.setUseParentHandlers(false);
            logger.info("Logging to file begins:");
        } catch (SecurityException | IOException e) {
            logger.severe("Can't create log file, logging to console");
            e.printStackTrace();
        }
        return logger;
    }
}
