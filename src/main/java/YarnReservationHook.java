import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.hooks.ExecuteWithHookContext;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationSubmissionRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationSubmissionResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.YarnException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.logging.Logger;

public class YarnReservationHook implements ExecuteWithHookContext {
    private static final Logger logger = Logger.getLogger(YarnReservationHook.class.getName());
    private final String RESERVATION_FILE_NAME = "reservation.file";
    private final String MR_RESERVATION_PROPERTY = "mapreduce.job.reservation.id"; // redefining from MRJob
    private final String TEZ_RESERVATION_PROPERTY = "tez.queue.name"; // redefining from MRJob
    private final String RESERVATION_QUEUE = "joblauncher"; // redefining from MRJob

    public void run(HookContext hookContext) throws Exception {
        System.out.println("Hello from Hook");
        try {
            Configuration conf = hookContext.getConf();
            String filename = conf.get(RESERVATION_FILE_NAME);
//            if(filename != null){
//                String jsonString = new String(Files.readAllBytes(Paths.get(filename)));
//                String reservationID = ""; // = makeRestCall(jsonString);
                // Example Reservation and running through reservation
                long arrival = System.currentTimeMillis()+10*1000; // extra 10 secs so that YARN is able to find a reservation.
                long duration = 1*60*1000;
                long deadline = arrival + duration + 60*1000;
                String reservationID = getReservation(arrival,deadline,duration,3,1024,1,1, RESERVATION_QUEUE);
                if (reservationID != null) {
                    String engine = conf.get("hive.execution.engine");
                    if(engine.equalsIgnoreCase("mr")) {
                        conf.set(MR_RESERVATION_PROPERTY, reservationID);
                    }else if (engine.equalsIgnoreCase("tez")){
                        conf.set(TEZ_RESERVATION_PROPERTY,reservationID);
                        logger.info("Setting the tez queue to be reserveion : " + reservationID);
                    }else {
                        logger.warning("Unknown Execution engine");
                    }
                    Thread.sleep(13*1000); // wait for reservation to be active
                }else{
                    logger.warning("Could not reserve resource according to "+filename);
                }
//            }
        }catch (Exception ignored){} // to make sure that we do not affect the main query
    }

    public String getReservation(long arrival, long deadline, long duration, int numContainers, int containerMem, int containerCpu, int gangSize, String queueName){
        YarnClient yarnClient = YarnClient.createYarnClient();
        yarnClient.init(new Configuration());
        yarnClient.start();
        List<ReservationRequest> reservation = new ArrayList<>();

        ReservationRequest stageReservation =  ReservationRequest.newInstance(Resource.newInstance(containerMem, containerCpu),numContainers, gangSize, duration);
        reservation.add(stageReservation);

        ReservationSubmissionRequest submissionRequest = null;
        ReservationId reservationID = null;
        try {
            submissionRequest = ReservationSubmissionRequest.newInstance(
                    ReservationDefinition.newInstance(arrival, deadline,
                            ReservationRequests.newInstance(reservation, ReservationRequestInterpreter.R_ORDER), null), queueName);

            ReservationSubmissionResponse response = yarnClient.submitReservation(submissionRequest);
            logger.info("[perforator] Reservation "+ response.getReservationId()+ " from ( "+ new Date(arrival) + ") to (" + new Date(deadline) + ")for "+reservation);
            reservationID = response.getReservationId();
        }catch (YarnException e){
            logger.info("Error while trying to get a reservation." + e);
        } catch (IOException e) {
            e.printStackTrace();
        }

        if (reservationID != null) {
            return reservationID.toString();
        }
        logger.fine("Reservation attempt failed. Running as best effort");
        return null;
    }

}
