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

package com.github.jsgilmore.protoshell;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import backtype.storm.multilang.ShellMsg;
import backtype.storm.multilang.ISerializer;
import backtype.storm.multilang.BoltMsg;
import backtype.storm.multilang.NoOutputException;
import backtype.storm.multilang.SpoutMsg;
import backtype.storm.task.TopologyContext;

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;

public class ProtoSerializer implements ISerializer {
    public static Logger LOG = Logger.getLogger(ProtoSerializer.class);
	private DataOutputStream processIn;
	private InputStream processOut;

	public void initialize(OutputStream processIn, InputStream processOut) {
		this.processIn = new DataOutputStream(processIn);
        this.processOut = processOut;
	}

	public Number connect(Map conf, TopologyContext context) throws IOException, NoOutputException {
        ShellMessages.Context.Builder setupInfo = ShellMessages.Context.newBuilder()
        		.setPidDir(context.getPIDDir());

        Set<Map.Entry> entries = conf.entrySet();
        ShellMessages.Conf.Builder confRecBuilder = ShellMessages.Conf.newBuilder();
        for (Map.Entry entry : entries)
        {
        	if (entry.getValue() != null) {
	        	ShellMessages.Conf confRec = confRecBuilder
	            		.setKey(entry.getKey().toString())
	            		.setValue(entry.getValue().toString())
	            		.build();
	            setupInfo.addConfs(confRec);
        	}
        }

        ShellMessages.Topology.Builder topologyBuilder = ShellMessages.Topology.newBuilder()
        		.setTaskId(context.getThisTaskId());
        ShellMessages.TaskComponentMapping.Builder mappingBuilder = ShellMessages.TaskComponentMapping.newBuilder();
        for (Map.Entry<Integer, String> entry : context.getTaskToComponent().entrySet()) {
        	ShellMessages.TaskComponentMapping mapping = mappingBuilder
        			.setTask(entry.getKey().toString())
        			.setComponent(entry.getValue())
        			.build();
        	topologyBuilder.addTaskComponentMappings(mapping);
        }
        setupInfo.setTopology(topologyBuilder.build());

        ShellMessages.Context setupMsg = setupInfo.build();
        LOG.info("Writing configuration to shell component");
        writeMessage(setupMsg);

        LOG.info("Waiting for pid from component");
        ShellMessages.Pid pidMsg = (ShellMessages.Pid)readMessage(ShellMessages.Pid.PARSER);
        LOG.info("Shell component connection established.");
        return (Number)pidMsg.getPid();
	}

	public ShellMsg readShellMsg() throws IOException, NoOutputException {
		ShellMessages.ShellMsgProto emissionProto = (ShellMessages.ShellMsgProto)readMessage(ShellMessages.ShellMsgProto.PARSER);
		ShellMsg shellMsg = new ShellMsg();
		ShellMessages.ShellMsgMeta meta = emissionProto.getShellMsgMeta();

		shellMsg.setAnchors(meta.getAnchorsList());
		shellMsg.setCommand(meta.getCommand());
		shellMsg.setId(meta.getId());
		shellMsg.setMsg(meta.getMsg());
		shellMsg.setStream(meta.getStream());
		shellMsg.setTask(meta.getTask());
        shellMsg.setNeedTaskIds(meta.getNeedTaskIds());

		// Java protocol buffers encode bytes in their ByteStrong format
		// Kryo cannot serialize ByteStrings
		// TODO Get Kryo to serialise ByteStrings
		for (ByteString o: emissionProto.getContentsList()) {
		    shellMsg.addTuple(o.toByteArray());
		}
		return shellMsg;
	}

	public void writeBoltMsg(BoltMsg boltMsg) throws IOException {
		ShellMessages.BoltMsgMeta meta = ShellMessages.BoltMsgMeta.newBuilder()
    			.setId(boltMsg.getId())
    			.setComp(boltMsg.getComp())
    			.setStream(boltMsg.getStream())
    			.setTask(boltMsg.getTask())
    			.build();
    	ShellMessages.BoltMsgProto.Builder tupleBuilder = ShellMessages.BoltMsgProto.newBuilder()
    			.setBoltMsgMeta(meta);
    	for (Object object: boltMsg.getTuple()) {
    	    ByteString byteString = ByteString.copyFrom((byte[])object);
            tupleBuilder.addContents(byteString);
    	}
        writeMessage(tupleBuilder.build());
	}

	public void writeSpoutMsg(SpoutMsg msg) throws IOException {
		ShellMessages.SpoutMsg.Builder spoutProto = ShellMessages.SpoutMsg.newBuilder();
		if (msg.getCommand() == "next") {
			spoutProto.setCommand("next");
			spoutProto.clearId();
		} else {
			spoutProto.setCommand(msg.getCommand());
			spoutProto.setId(msg.getId());
		}
        writeMessage(spoutProto.build());
	}

	public void writeTaskIds(List<Integer> taskIds) throws IOException {
		ShellMessages.TaskIds.Builder tasksProto = ShellMessages.TaskIds.newBuilder();
		for (Integer taskId : taskIds) {
			tasksProto.addTaskIds(taskId);
		}
        writeMessage(tasksProto.build());
	}

	private void writeMessage(Message msg) throws IOException {
        msg.writeDelimitedTo(processIn);
        processIn.flush();
    }

	private Object readMessage(Parser parser) throws IOException {
	    Object message = parser.parseDelimitedFrom(processOut);
	    if (message == null) {
	        throw new RuntimeException("Shell process died");
	    }
	    return message;
    }
}
