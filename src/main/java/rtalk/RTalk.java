package rtalk;

/**
 * <pre>

Job Lifecycle
-------------

A job in beanstalk gets created by a client with the "put" command. During its
life it can be in one of four states: "ready", "reserved", "delayed", or
"buried". After the put command, a job typically starts out ready. It waits in
the ready queue until a worker comes along and runs the "reserve" command. If
this job is next in the queue, it will be reserved for the worker. The worker
will execute the job; when it is finished the worker will send a "delete"
command to delete the job.

Here is a picture of the typical job lifecycle:

   put            reserve               delete
  -----> [READY] ---------> [RESERVED] --------> *poof*

Here is a picture with more possibilities:



   put with delay               release with delay
  ----------------> [DELAYED] <------------.
                        |                   |
                        | (time passes)     |
                        |                   |
   put                  v     reserve       |       delete
  -----------------> [READY] ---------> [RESERVED] --------> *poof*
                       ^  ^                |  |
                       |   \  release      |  |
                       |    `-------------'   |
                       |                      |
                       | kick                 |
                       |                      |
                       |       bury           |
                    [BURIED] <---------------'
                       |
                       |  delete
                        `--------> *poof*


The system has one or more tubes. Each tube consists of a ready queue and a
delay queue. Each job spends its entire life in one tube. Consumers can show
interest in tubes by sending the "watch" command; they can show disinterest by
sending the "ignore" command. This set of interesting tubes is said to be a
consumer's "watch list". When a client reserves a job, it may come from any of
the tubes in its watch list.

When a client connects, its watch list is initially just the tube named
"default". If it submits jobs without having sent a "use" command, they will
live in the tube named "default".

Tubes are created on demand whenever they are referenced. If a tube is empty
(that is, it contains no ready, delayed, or buried jobs) and no client refers
to it, it will be deleted.

 * </pre>
 * 
 * @author zhukov
 *
 */
public class RTalk {
    private String tube;

    /**
     * use <tube>\r\n
     * 
     * - <tube> is a name at most 200 bytes. It specifies the tube to use. If
     * the tube does not exist, it will be created.
     * 
     * The only reply is:
     * 
     * USING <tube>\r\n
     * 
     * - <tube> is the name of the tube now being used.
     */
    public String use(String tube) {
        this.tube = tube;
        return tube;
    }

    /**
     * put <pri> <delay> <ttr> <bytes>\r\n
     * 
     * <data>\r\n
     * 
     * It inserts a job into the client's currently used tube (see the "use"
     * command below).
     * 
     * - <pri> is an integer < 2**32. Jobs with smaller priority values will be
     * scheduled before jobs with larger priorities. The most urgent priority is
     * 0; the least urgent priority is 4,294,967,295.
     * 
     * - <delay> is an integer number of seconds to wait before putting the job
     * in the ready queue. The job will be in the "delayed" state during this
     * time.
     * 
     * - <ttr> -- time to run -- is an integer number of seconds to allow a
     * worker to run this job. This time is counted from the moment a worker
     * reserves this job. If the worker does not delete, release, or bury the
     * job within <ttr> seconds, the job will time out and the server will
     * release the job. The minimum ttr is 1. If the client sends 0, the server
     * will silently increase the ttr to 1.
     * 
     * - <bytes> is an integer indicating the size of the job body, not
     * including the trailing "\r\n". This value must be less than max-job-size
     * (default: 2**16).
     * 
     * - <data> is the job body -- a sequence of bytes of length <bytes> from
     * the previous line.
     * 
     * After sending the command line and body, the client waits for a reply,
     * which may be:
     * 
     * - "INSERTED <id>\r\n" to indicate success.
     * 
     * - <id> is the integer id of the new job
     * 
     * - "BURIED <id>\r\n" if the server ran out of memory trying to grow the
     * priority queue data structure.
     * 
     * - <id> is the integer id of the new job
     * 
     * - "EXPECTED_CRLF\r\n" The job body must be followed by a CR-LF pair, that
     * is, "\r\n". These two bytes are not counted in the job size given by the
     * client in the put command line.
     * 
     * - "JOB_TOO_BIG\r\n" The client has requested to put a job with a body
     * larger than max-job-size bytes.
     * 
     * - "DRAINING\r\n" This means that the server has been put into
     * "drain mode" and is no longer accepting new jobs. The client should try
     * another server or disconnect and try again later.
     * 
     * The "use" command is for producers. Subsequent put commands will put jobs
     * into the tube specified by this command. If no use command has been
     * issued, jobs will be put into the tube named "default".
     */
    public static class PutResponse {
        public String status;
        public String id;
    }

    public PutResponse put(long pri, long delay, long ttr, String data) {
        return new PutResponse();
    }

    /**
     * 
     * A process that wants to consume jobs from the queue uses "reserve",
     * "delete", "release", and "bury". The first worker command, "reserve",
     * looks like this:
     * 
     * reserve\r\n
     * 
     * Alternatively, you can specify a timeout as follows:
     * 
     * reserve-with-timeout <seconds>\r\n
     * 
     * This will return a newly-reserved job. If no job is available to be
     * reserved, beanstalkd will wait to send a response until one becomes
     * available. Once a job is reserved for the client, the client has limited
     * time to run (TTR) the job before the job times out. When the job times
     * out, the server will put the job back into the ready queue. Both the TTR
     * and the actual time left can be found in response to the stats-job
     * command.
     * 
     * If more than one job is ready, beanstalkd will choose the one with the
     * smallest priority value. Within each priority, it will choose the one
     * that was received first.
     * 
     * A timeout value of 0 will cause the server to immediately return either a
     * response or TIMED_OUT. A positive value of timeout will limit the amount
     * of time the client will block on the reserve request until a job becomes
     * available.
     * 
     * During the TTR of a reserved job, the last second is kept by the server
     * as a safety margin, during which the client will not be made to wait for
     * another job. If the client issues a reserve command during the safety
     * margin, or if the safety margin arrives while the client is waiting on a
     * reserve command, the server will respond with:
     * 
     * DEADLINE_SOON\r\n
     * 
     * This gives the client a chance to delete or release its reserved job
     * before the server automatically releases it.
     * 
     * TIMED_OUT\r\n
     * 
     * If a non-negative timeout was specified and the timeout exceeded before a
     * job became available, or if the client's connection is half-closed, the
     * server will respond with TIMED_OUT.
     * 
     * Otherwise, the only other response to this command is a successful
     * reservation in the form of a text line followed by the job body:
     * 
     * RESERVED <id> <bytes>\r\n <data>\r\n
     * 
     * - <id> is the job id -- an integer unique to this job in this instance of
     * beanstalkd.
     * 
     * - <bytes> is an integer indicating the size of the job body, not
     * including the trailing "\r\n".
     * 
     * - <data> is the job body -- a sequence of bytes of length <bytes> from
     * the previous line. This is a verbatim copy of the bytes that were
     * originally sent to the server in the put command for this job.
     */
    public ReserveResponse reserve() {
        return reserve(0);
    }

    public static class ReserveResponse {
        public String status;
        public String id;
        public String data;
    }

    public ReserveResponse reserve(long timeoutMsec) {
        ReserveResponse response = new ReserveResponse();
        return response;
    }

    /**
     * The delete command removes a job from the server entirely. It is normally
     * used by the client when the job has successfully run to completion. A
     * client can delete jobs that it has reserved, ready jobs, delayed jobs,
     * and jobs that are buried. The delete command looks like this:
     * 
     * delete <id>\r\n
     * 
     * - <id> is the job id to delete.
     * 
     * The client then waits for one line of response, which may be:
     * 
     * - "DELETED\r\n" to indicate success.
     * 
     * - "NOT_FOUND\r\n" if the job does not exist or is not either reserved by
     * the client, ready, or buried. This could happen if the job timed out
     * before the client sent the delete command.
     */
    public String delete(String id) {
        return "NOT_FOUND";
    }

    /**
     * The release command puts a reserved job back into the ready queue (and
     * marks its state as "ready") to be run by any client. It is normally used
     * when the job fails because of a transitory error. It looks like this:
     * 
     * release <id> <pri> <delay>\r\n
     * 
     * - <id> is the job id to release.
     * 
     * - <pri> is a new priority to assign to the job.
     * 
     * - <delay> is an integer number of seconds to wait before putting the job
     * in the ready queue. The job will be in the "delayed" state during this
     * time.
     * 
     * The client expects one line of response, which may be:
     * 
     * - "RELEASED\r\n" to indicate success.
     * 
     * - "BURIED\r\n" if the server ran out of memory trying to grow the
     * priority queue data structure.
     * 
     * - "NOT_FOUND\r\n" if the job does not exist or is not reserved by the
     * client.
     */
    public String release(String id) {
        return "NOT_FOUND";
    }

    /**
     * The bury command puts a job into the "buried" state. Buried jobs are put
     * into a FIFO linked list and will not be touched by the server again until
     * a client kicks them with the "kick" command.
     * 
     * The bury command looks like this:
     * 
     * bury <id> <pri>\r\n
     * 
     * - <id> is the job id to release.
     * 
     * - <pri> is a new priority to assign to the job.
     * 
     * There are two possible responses:
     * 
     * - "BURIED\r\n" to indicate success.
     * 
     * - "NOT_FOUND\r\n" if the job does not exist or is not reserved by the
     * client.
     */
    public String bury(String id, long pri) {
        return "NOT_FOUND";
    }

    /**
     * The "touch" command allows a worker to request more time to work on a
     * job. This is useful for jobs that potentially take a long time, but you
     * still want the benefits of a TTR pulling a job away from an unresponsive
     * worker. A worker may periodically tell the server that it's still alive
     * and processing a job (e.g. it may do this on DEADLINE_SOON). The command
     * postpones the auto release of a reserved job until TTR seconds from when
     * the command is issued.
     * 
     * The touch command looks like this:
     * 
     * touch <id>\r\n
     * 
     * - <id> is the ID of a job reserved by the current connection.
     * 
     * There are two possible responses:
     * 
     * - "TOUCHED\r\n" to indicate success.
     * 
     * - "NOT_FOUND\r\n" if the job does not exist or is not reserved by the
     * client.
     */
    public String touch(String id) {
        return "NOT_FOUND";
    }

}
