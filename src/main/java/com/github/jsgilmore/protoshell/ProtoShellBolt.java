//   Copyright 2013 Vastech SA (PTY) LTD
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.

package main.java.com.github.jsgilmore.protoshell;

import backtype.storm.generated.ShellComponent;
import backtype.storm.task.IBolt;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import static java.util.concurrent.TimeUnit.SECONDS;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;

/**
 * A bolt that shells out to another process to process tuples. ShellBolt
 * communicates with that process over stdio using a special protocol. An ~100
 * line library is required to implement that protocol, and adapter libraries
 * currently exist for Ruby and Python.
 *
 * <p>To run a ShellBolt on a cluster, the scripts that are shelled out to must be
 * in the resources directory within the jar submitted to the master.
 * During development/testing on a local machine, that resources directory just
 * needs to be on the classpath.</p>
 *
 * <p>When creating topologies using the Java API, subclass this bolt and implement
 * the IRichBolt interface to create components for the topology that use other languages. For example:
 * </p>
 *
 * <pre>
 * public class MyBolt extends ShellBolt implements IRichBolt {
 *      public MyBolt() {
 *          super("python", "mybolt.py");
 *      }
 *
 *      public void declareOutputFields(OutputFieldsDeclarer declarer) {
 *          declarer.declare(new Fields("field1", "field2"));
 *      }
 * }
 * </pre>
 */
public class ProtoShellBolt implements IBolt {
    public static Logger LOG = LoggerFactory.getLogger(ProtoShellBolt.class);
    Process _subprocess;
    OutputCollector _collector;
    Map<String, Tuple> _inputs = new ConcurrentHashMap<String, Tuple>();

    private String[] _command;
    private ProtoShellProcess _process;
    private volatile boolean _running = true;
    private volatile Throwable _exception;
    private LinkedBlockingQueue _pendingWrites = new LinkedBlockingQueue();
    private Random _rand;
    
    private Thread _readerThread;
    private Thread _writerThread;
    
    private ShellMessages.EmissionProto _emission;

    public ProtoShellBolt(ShellComponent component) {
        this(component.get_execution_command(), component.get_script());
    }

    public ProtoShellBolt(String... command) {
        _command = command;
    }

    public void prepare(Map stormConf, TopologyContext context,
                        final OutputCollector collector) {
        _rand = new Random();
        _process = new ProtoShellProcess(_command);
        _collector = collector;

        try {
            //subprocesses must send their pid first thing
            Number subpid = _process.launch(stormConf, context);
            LOG.info("Launched subprocess with pid " + subpid);
        } catch (IOException e) {
            throw new RuntimeException("Error when launching multilang subprocess\n" + _process.getErrorsString(), e);
        }

        // reader
        _readerThread = new Thread(new Runnable() {
            public void run() {
                while (_running) {
                    try {
                    	_emission = (ShellMessages.EmissionProto)_process.readMessage(ShellMessages.EmissionProto.PARSER);

                        String command = _emission.getEmissionMetadata().getCommand();
                        if(command.equals("ack")) {
                            handleAck(_emission.getEmissionMetadata().getId());
                        } else if (command.equals("fail")) {
                            handleFail(_emission.getEmissionMetadata().getId());
                        } else if (command.equals("error")) {
                            handleError(_emission.getEmissionMetadata().getMsg());
                        } else if (command.equals("log")) {
                            String msg = _emission.getEmissionMetadata().getMsg();
                            LOG.info("Shell msg: " + msg);
                        } else if (command.equals("emit")) {
                            handleEmit(_emission);
                        }
                    } catch (InterruptedException e) {
                    } catch (Throwable t) {
                        die(t);
                    }
                }
            }
        });
        
        _readerThread.start();

        _writerThread = new Thread(new Runnable() {
            public void run() {
                while (_running) {
                    try {
                        Object write = _pendingWrites.poll(1, SECONDS);
                        if (write != null) {
                            _process.writeMessage((Message)write);
                        }
                        // drain the error stream to avoid dead lock because of full error stream buffer
                        _process.drainErrorStream();
                    } catch (InterruptedException e) {
                    } catch (Throwable t) {
                        die(t);
                    }
                }
            }
        });
        
        _writerThread.start();
    }

    public void execute(Tuple input) {
        if (_exception != null) {
            throw new RuntimeException(_exception);
        }

        //just need an id
        String genId = Long.toString(_rand.nextLong());
        _inputs.put(genId, input);
        try {
        	ShellMessages.TupleMetadata tupleMetadata = ShellMessages.TupleMetadata.newBuilder()
        			.setId(genId)
        			.setComp(input.getSourceComponent())
        			.setStream(input.getSourceStreamId())
        			.setTask(input.getSourceTask())
        			.build();
        	ShellMessages.TupleProto.Builder tupleBuilder = ShellMessages.TupleProto.newBuilder()
        			.setTupleMetadata(tupleMetadata);
        	for (Object object: input.getValues()) {
        		ByteString byteString = ByteString.copyFrom((byte[])object);
        		tupleBuilder.addContents(byteString);
        	}
            _pendingWrites.put(tupleBuilder.build());
        } catch(InterruptedException e) {
            throw new RuntimeException("Error during multilang processing", e);
        }
    }

    public void cleanup() {
        _running = false;
        _process.destroy();
        _inputs.clear();
    }

    private void handleAck(String id) {
        Tuple acked = _inputs.remove(id);
        if(acked==null) {
            throw new RuntimeException("Acked a non-existent or already acked/failed id: " + id);
        }
        _collector.ack(acked);
    }

    private void handleFail(String id) {
        Tuple failed = _inputs.remove(id);
        if(failed==null) {
            throw new RuntimeException("Failed a non-existent or already acked/failed id: " + id);
        }
        _collector.fail(failed);
    }

    private void handleError(String msg) {
        _collector.reportError(new Exception("Shell Process Exception: " + msg));
    }

    private void handleEmit(ShellMessages.EmissionProto emission) throws InterruptedException {
    	ShellMessages.EmissionMetadata meta = emission.getEmissionMetadata();
        String stream = meta.getStream();
        if(stream==null) stream = Utils.DEFAULT_STREAM_ID;
        Long task = meta.getTask();
        
        List<Object> tuple = new ArrayList<Object>();
        for (ByteString content: emission.getContentsList()) {
        	tuple.add(content.toByteArray());
        }
        
        List<Tuple> anchors = new ArrayList<Tuple>();
        List<String> anchorsList = meta.getAnchorsList();
        if(anchorsList.size() > 0) {
            for(String anchor: anchorsList) {
                Tuple t = _inputs.get(anchor);
                if (t == null) {
                    throw new RuntimeException("Anchored onto " + anchor + " after ack/fail");
                }
                anchors.add(t);
            }
        }
        
        if(task==null) {
            List<Integer> outtasks = _collector.emit(stream, anchors, tuple);
            _pendingWrites.put(outtasks);
        } else {
            _collector.emitDirect((int)task.longValue(), stream, anchors, tuple);
        }
    }

    private void die(Throwable exception) {
        _exception = exception;
    }
}
