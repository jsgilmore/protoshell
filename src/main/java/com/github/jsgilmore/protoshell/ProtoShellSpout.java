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
import backtype.storm.spout.ISpout;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.utils.Utils;

import java.util.ArrayList;
import java.util.Map;
import java.util.List;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;

public class ProtoShellSpout implements ISpout {
    public static Logger LOG = LoggerFactory.getLogger(ProtoShellSpout.class);

    private SpoutOutputCollector _collector;
    private String[] _command;
    private ProtoShellProcess _process;
    
    private ShellMessages.SpoutMsg.Builder _spoutMsg;
    private ShellMessages.EmissionProto _emission;

    public ProtoShellSpout(ShellComponent component) {
        this(component.get_execution_command(), component.get_script());
    }
    
    public ProtoShellSpout(String... command) {
        _command = command;
    }
    
    public void open(Map stormConf, TopologyContext context,
                     SpoutOutputCollector collector) {
        _process = new ProtoShellProcess(_command);
        _collector = collector;

        try {
            Number subpid = _process.launch(stormConf, context);
            LOG.info("Launched subprocess with pid " + subpid);
        } catch (IOException e) {
            throw new RuntimeException("Error when launching multilang subprocess\n" + _process.getErrorsString(), e);
        }
    }

    public void close() {
        _process.destroy();
    }
    
    public void nextTuple() {
    	if (_spoutMsg == null) {
    		_spoutMsg = ShellMessages.SpoutMsg.newBuilder();
    	}
        _spoutMsg.setCommand("next");
        _spoutMsg.clearId();
        querySubprocess(_spoutMsg.build());
    }

    public void ack(Object msgId) {
    	if (_spoutMsg == null) {
    		_spoutMsg = ShellMessages.SpoutMsg.newBuilder();
    	}
        _spoutMsg.setCommand("ack");
        _spoutMsg.setId(msgId.toString());
        querySubprocess(_spoutMsg.build());
    }


    public void fail(Object msgId) {
    	if (_spoutMsg == null) {
    		_spoutMsg = ShellMessages.SpoutMsg.newBuilder();
    	}
    	_spoutMsg.setCommand("fail");
        _spoutMsg.setId(msgId.toString());
        querySubprocess(_spoutMsg.build());
    }

    private void querySubprocess(Message spoutMsg) {
        try {
            _process.writeMessage(spoutMsg);

            while (true) {
            	_emission = (ShellMessages.EmissionProto)_process.readMessage(ShellMessages.EmissionProto.PARSER);
            	String command = _emission.getEmissionMetadata().getCommand();
                if (command.equals("sync")) {
                    return;
                } else if (command.equals("log")) {
                	String msg = _emission.getEmissionMetadata().getMsg();
                } else if (command.equals("emit")) {
                	ShellMessages.EmissionMetadata meta = _emission.getEmissionMetadata();
                    String stream = meta.getStream();
                    if (stream == null) stream = Utils.DEFAULT_STREAM_ID;
                    Long task = meta.getTask();
                    
                    List<Object> tuple = new ArrayList<Object>();
                    for (ByteString content: _emission.getContentsList()) {
                    	tuple.add(content.toByteArray());
                    }
                    
                    String messageId = meta.getId();
                    if (task == 0) {
                        List<Integer> outtasks = _collector.emit(stream, tuple, messageId);
                        ShellMessages.TaskIds.Builder taskIdsBuilder = ShellMessages.TaskIds.newBuilder(); 
                        for (Integer outtask: outtasks) {
                        	taskIdsBuilder.addTaskIds(outtask);
                        }
                        _process.writeMessage(taskIdsBuilder.build());
                    } else {
                        _collector.emitDirect((int)task.longValue(), stream, tuple, messageId);
                    }
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void activate() {
    }

    @Override
    public void deactivate() {
    }
}
