package rtalk;

import static java.lang.Long.signum;
import static java.util.Base64.getUrlEncoder;
import static java.util.UUID.randomUUID;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Transaction;

public class RTalk extends RedisDao {

    public static final String KICKED = "KICKED";
    public static final String DELETED = "DELETED";
    public static final String TOUCHED = "TOUCHED";
    public static final String BURIED = "BURIED";
    public static final String RELEASED = "RELEASED";
    public static final String NOT_FOUND = "NOT_FOUND";
    public static final String RESERVED = "RESERVED";
    public static final String TIMED_OUT = "TIMED_OUT";
    public static final String DEADLINE_SOON = "DEADLINE_SOON";
    public static final String INSERTED = "INSERTED";
    public static final String EXPECTED_CRLF = "EXPECTED_CRLF";
    public static final String JOB_TOO_BIG = "JOB_TOO_BIG";
    public static final String DRAINING = "DRAINING";

    public RTalk(JedisPool jedis) {
        this(jedis, "default");
    }

    public RTalk(JedisPool jedis, String tube) {
        super(jedis);
        this.tube = tube;
    }

    protected final String tube;

    public String getTube() {
        return tube;
    }

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
    public static RTalk use(JedisPool jedis, String tube) {
        return new RTalk(jedis, tube);
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
    public static class Job {
        public static final String DELAYED = "DELAYED";
        public static final String READY = "READY";
        public static final String RESERVED = "RESERVED";
        public static final String BURIED = "BURIED";

        public String id;
        public long ttrMsec;
        public String data;
        public String state;
        public long pri;
        public String tube;
        public long reserves;
        public long releases;
        public long buries;
        public long kicks;
        public long timeouts;
        public long readyTime;
        public long ctime;
        public long now;

        /**
         * - "age" is the time in seconds since the put command that created
         * this job.
         */
        public long age() {
            return now - ctime;
        }

        /**
         * - "time-left" is the number of seconds left until the server puts
         * this job into the ready queue. This number is only meaningful if the
         * job is reserved or delayed. If the job is reserved and this amount of
         * time
         */
        public long timeLeft() {
            return readyTime - now;
        }

    }

    public Response put(long pri, long delayMsec, long ttrMsec, String data) {
        String id = newId();
        return putWithId(id, pri, delayMsec, ttrMsec, data);
    }

    protected String newId() {
        UUID randomUUID = randomUUID();
        return randomUUID.toString();
    }

    private String newIdBase64() {
        UUID randomUUID = randomUUID();
        ByteBuffer buf = ByteBuffer.allocate(16);
        buf.putLong(randomUUID.getMostSignificantBits());
        buf.putLong(randomUUID.getLeastSignificantBits());

        byte[] encode = getUrlEncoder().encode(buf.array());
        String str = null;
        if (encode[encode.length - 1] == '=' && encode[encode.length - 2] == '=') {
            str = new String(encode, 0, encode.length - 2);
        } else {
            str = new String(encode);
        }
        return str;
    }

    public Response putWithId(String id, long pri, long delayMsec, long ttrMsec, String data) {
        if (contains(id)) {
            return bury(id, pri);
        }
        long _ttrMsec = Math.max(1000, ttrMsec);
        String status = delayMsec > 0 ? Job.DELAYED : Job.READY;
        return withRedisTransaction(r -> {
            long now = System.currentTimeMillis();
            long readyTimeMsec = now + delayMsec;
            r.zadd(kReadyQueue(), readyTimeMsec, id);
            r.hset(kJob(id), fPriority, Long.toString(pri));
            r.hset(kJob(id), fTtr, Long.toString(_ttrMsec));
            r.hset(kJob(id), fData, data);
            r.hset(kJob(id), fState, status);
            r.hset(kJob(id), fCtime, Long.toString(now));
            r.hset(kJob(id), fTube, tube);
            return on(new Response(INSERTED, id, data));
        });
    }

    protected Response on(Response response) {
        return response;
    }

    private static final String fTube = "tube";
    private static final String fState = "state";
    private static final String fPriority = "pri";
    private static final String fReserves = "reserves";
    private static final String fCtime = "ctime";
    private static final String fTtr = "ttr";
    private static final String fData = "data";
    private static final String fTimeouts = "timeouts";
    private static final String fReleases = "releases";
    private static final String fBuries = "buries";
    private static final String fKicks = "kicks";

    private String kJob(String id) {
        return tube + "_" + id;
    }

    private String kReadyQueue() {
        return tube + "_readyQueue";
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
    public Response reserve() {
        return reserve(0);
    }

    public class Response {
        public final String tube;

        public Response(String status, String id) {
            this.status = status;
            this.id = id;
            this.tube = getTube();
        }

        public Response(String status, String id, String data) {
            this.status = status;
            this.id = id;
            this.data = data;
            this.tube = getTube();
        }

        public String status;
        public String id;
        public String data;

        public boolean isReserved() {
            return RESERVED.equals(status);
        }

        public boolean isInserted() {
            return INSERTED.equals(status);
        }

        public boolean isDeleted() {
            return DELETED.equals(status);
        }

        public boolean isBuried() {
            return BURIED.equals(status);
        }

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            builder.append("{");
            if (tube != null) {
                builder.append("\"tube\": '");
                builder.append(tube);
                builder.append("'");
            }

            if (status != null) {
                builder.append(", \"status\": '");
                builder.append(status);
                builder.append("'");
            }
            if (id != null) {
                builder.append(", \"id\": '");
                builder.append(id);
                builder.append("'");
            }
            if (data != null) {
                builder.append(", \"data\": '");
                builder.append(data);
                builder.append("'");
            }
            builder.append("}");
            return builder.toString();
        }
    }

    public Response reserve(long blockTimeoutMsec) {
        long now = System.currentTimeMillis();
        Optional<Job> firstJob = withRedis(r -> {
            Set<String> ids = r.zrangeByScore(kReadyQueue(), 0, now);
            Optional<Job> firstJob_ = ids.stream()
                                         .map(id -> _getJob(r, id))
                                         .filter(j -> j != null && !Job.BURIED.equals(j.state))
                                         .sorted((j1, j2) -> signum(j1.pri - j2.pri))
                                         .findFirst();
            return firstJob_;
        });

        if (firstJob.isPresent()) {
            Job j = firstJob.get();
            return withRedisTransaction(r -> {
                r.hset(kJob(j.id), fState, Job.RESERVED);
                r.zadd(kReadyQueue(), now + j.ttrMsec, j.id);
                r.hincrBy(kJob(j.id), fReserves, 1);
                return on(new Response(RESERVED, j.id, j.data));
            });
        }
        return new Response(TIMED_OUT, null, null);
    }

    private Job _getJob(Jedis r, String id) {
        long now = System.currentTimeMillis();
        Map<String, String> job = r.hgetAll(kJob(id));
        if (job == null || job.isEmpty())
            return null;
        Job j = new Job();
        Double readyTime = r.zscore(kReadyQueue(), id);
        if (readyTime != null) {
            j.readyTime = readyTime.longValue();
        }
        j.tube = job.get(fTube);
        j.state = job.get(fState);
        j.pri = toLong(job.get(fPriority));
        j.data = job.get(fData);
        j.ttrMsec = toLong(job.get(fTtr));
        j.id = id;
        j.reserves = toLong(job.get(fReserves));
        j.releases = toLong(job.get(fReleases));
        j.buries = toLong(job.get(fBuries));
        j.kicks = toLong(job.get(fKicks));
        j.timeouts = toLong(job.get(fTimeouts));
        j.ctime = toLong(job.get(fCtime));
        j.now = now;
        return j;
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
    public Response delete(String id) {
        Double score = withRedis(r -> r.zscore(kReadyQueue(), id));
        if (score == null) {
            return new Response(NOT_FOUND, id);
        }
        return withRedisTransaction(r -> {
            r.zrem(kReadyQueue(), id);
            r.del(kJob(id));
            return on(new Response(DELETED, id));
        });
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
    public Response release(String id, long pri, long delayMsec) {
        if (contains(id)) {
            return withRedisTransaction(tx -> {
                tx.zadd(kReadyQueue(), System.currentTimeMillis() + delayMsec, id);
                tx.hset(kJob(id), fPriority, Long.toString(pri));
                tx.hset(kJob(id), fState, delayMsec > 0 ? Job.DELAYED : Job.READY);
                tx.hincrBy(kJob(id), fReleases, 1);
                return new Response(RELEASED, id);
            });
        }
        return new Response(NOT_FOUND, id);
    }

    public boolean contains(String id) {
        return withRedis(r -> r.exists(kJob(id)));
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
    public Response bury(String id, long pri) {
        if (contains(id)) {
            return withRedisTransaction(tx -> {
                tx.zrem(kReadyQueue(), id);
                tx.hset(kJob(id), fPriority, Long.toString(pri));
                tx.hset(kJob(id), fState, Job.BURIED);
                tx.hincrBy(kJob(id), fBuries, 1);
                tx.zadd(kBuried(), System.currentTimeMillis(), id);
                return new Response(BURIED, id);
            });
        }
        return new Response(NOT_FOUND, id);
    }

    private String kBuried() {
        return tube + "_buried";
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
    public Response touch(String id) {
        Job j = withRedis(r -> _getJob(r, id));
        if (j != null) {
            withRedis(r -> {
                r.zincrby(kReadyQueue(), j.ttrMsec, id);
                return new Response(TOUCHED, id);
            });
        }
        return new Response(NOT_FOUND, id);
    }

    public static long toLong(Object object) {
        return toLong(object, 0);
    }

    public static long toLong(Object object, long defaultValue) {
        if (object == null) {
            return defaultValue;
        }
        if (object instanceof Number) {
            return ((Number) object).longValue();
        } else if (object instanceof String) {
            try {
                return Long.parseLong((String) object);
            } catch (NumberFormatException nfe) {
                return defaultValue;
            }
        }
        return defaultValue;
    }

    /**
     * The kick command applies only to the currently used tube. It moves jobs
     * into the ready queue. If there are any buried jobs, it will only kick
     * buried jobs. Otherwise it will kick delayed jobs. It looks like:
     * 
     * kick <bound>\r\n
     * 
     * - <bound> is an integer upper bound on the number of jobs to kick. The
     * server will kick no more than <bound> jobs.
     * 
     * The response is of the form:
     * 
     * KICKED <count>\r\n
     * 
     * - <count> is an integer indicating the number of jobs actually kicked.
     */
    public int kick(int bound) {
        long now = System.currentTimeMillis();
        Set<String> ids = withRedis(r -> {
            if (r.zcard(kBuried()) > 0) {
                return r.zrange(kBuried(), 0, bound);
            } else {
                return r.zrangeByScore(kReadyQueue(), now, -1, 0, bound);
            }
        });
        if (ids.isEmpty())
            return 0;

        updateRedisTransaction(tx -> {
            for (String id : ids) {
                _kickJob(id, now, tx);
            }
        });
        return ids.size();
    }

    /**
     * The kick-job command is a variant of kick that operates with a single job
     * identified by its job id. If the given job id exists and is in a buried
     * or delayed state, it will be moved to the ready queue of the the same
     * tube where it currently belongs. The syntax is:
     * 
     * kick-job <id>\r\n
     * 
     * - <id> is the job id to kick.
     * 
     * The response is one of:
     * 
     * - "NOT_FOUND\r\n" if the job does not exist or is not in a kickable
     * state. This can also happen upon internal errors.
     * 
     * - "KICKED\r\n" when the operation succeeded.
     */
    public Response kickJob(String id) {
        if (isBurried(id)) {
            long now = System.currentTimeMillis();
            updateRedisTransaction(tx -> _kickJob(id, now, tx));
            return new Response(KICKED, id);
        }
        return new Response(NOT_FOUND, id);
    }

    private void _kickJob(String id, long now, Transaction tx) {
        tx.zrem(kBuried(), id);
        tx.hset(kJob(id), fState, Job.READY);
        tx.hincrBy(kJob(id), fKicks, 1);
        tx.zadd(kReadyQueue(), now, id);
        on(new Response(KICKED, id));
    }

    private boolean isBurried(String id) {
        return null != withRedis(r -> r.zscore(kBuried(), id));
    }

    /**
     * The stats-job command gives statistical information about the specified
     * job if it exists. Its form is:
     * 
     * stats-job <id>\r\n
     * 
     * - <id> is a job id.
     * 
     * The response is one of:
     * 
     * - "NOT_FOUND\r\n" if the job does not exist.
     * 
     * - "OK <bytes>\r\n<data>\r\n"
     * 
     * - <bytes> is the size of the following data section in bytes.
     * 
     * - <data> is a sequence of bytes of length <bytes> from the previous line.
     * It is a YAML file with statistical information represented a dictionary.
     * 
     * The stats-job data is a YAML file representing a single dictionary of
     * strings to scalars. It contains these keys:
     * 
     * - "id" is the job id
     * 
     * - "tube" is the name of the tube that contains this job
     * 
     * - "state" is "ready" or "delayed" or "reserved" or "buried"
     * 
     * - "pri" is the priority value set by the put, release, or bury commands.
     * 
     * - "age" is the time in seconds since the put command that created this
     * job.
     * 
     * - "time-left" is the number of seconds left until the server puts this
     * job into the ready queue. This number is only meaningful if the job is
     * reserved or delayed. If the job is reserved and this amount of time
     * elapses before its state changes, it is considered to have timed out.
     * 
     * - "file" is the number of the earliest binlog file containing this job.
     * If -b wasn't used, this will be 0.
     * 
     * - "reserves" is the number of times this job has been reserved.
     * 
     * - "timeouts" is the number of times this job has timed out during a
     * reservation.
     * 
     * - "releases" is the number of times a client has released this job from a
     * reservation.
     * 
     * - "buries" is the number of times this job has been buried.
     * 
     * - "kicks" is the number of times this job has been kicked.
     */

    public Job statsJob(String id) {
        return withRedis(r -> _getJob(r, id));
    }

}
