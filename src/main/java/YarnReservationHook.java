import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.hooks.ExecuteWithHookContext;
import org.apache.hadoop.hive.ql.hooks.HookContext;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.logging.Logger;

public class YarnReservationHook implements ExecuteWithHookContext {
    private static final Logger logger = Logger.getLogger(YarnReservationHook.class.getName());
    private final String RESERVATION_FILE_NAME = "reservation.file";
    private final String RESERVATION_PROPERTY = "mapreduce.job.reservation.id"; // redefining from MRJob

    public void run(HookContext hookContext) throws Exception {
        try {
            Configuration conf = hookContext.getConf();
            String filename = conf.get(RESERVATION_FILE_NAME);
            if(filename != null){
                String jsonString = new String(Files.readAllBytes(Paths.get(filename)));
                String reservationID = ""; // = makeRestCall(jsonString);
                if (reservationID != null) {
                    conf.set(RESERVATION_PROPERTY,reservationID);
                }else{
                    logger.warning("Could not reserve resource according to "+filename);
                }
            }
        }catch (Exception ignored){} // to make sure that we do not affect the main query
    }

}
