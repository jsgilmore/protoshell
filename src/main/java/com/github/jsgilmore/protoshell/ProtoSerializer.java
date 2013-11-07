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

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
	private DataOutputStream processIn;
	private InputStream processOut;

	@Override
	public void initialize(OutputStream processIn, InputStream processOut) {
		this.processIn = new DataOutputStream(processIn);
        this.processOut = processOut;
	}

	@Override
	public Number connect(Map conf, TopologyContext context)
			throws IOException, NoOutputException {

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

        writeMessage(setupInfo.build());

        ShellMessages.Pid pidMsg = (ShellMessages.Pid)readMessage(ShellMessages.Pid.PARSER);
        return (Number)pidMsg.getPid();
	}

	@Override
	public ShellMsg readShellMsg() throws IOException, NoOutputException {
		ShellMessages.ShellMsgProto emissionProto = (ShellMessages.ShellMsgProto)readMessage(ShellMessages.ShellMsgProto.PARSER);
		ShellMsg shellMsg = new ShellMsg();
		ShellMessages.ShellMsgMeta meta = emissionProto.getShellMsgMeta();

		List<String> anchors = meta.getAnchorsList();
		shellMsg.setAnchors(anchors);

		String command = meta.getCommand();
		shellMsg.setCommand(command);

		String id = meta.getId();
		shellMsg.setId(id);

		String msg = meta.getMsg();
		shellMsg.setMsg(msg);

		String stream = meta.getStream();
		shellMsg.setStream(stream);

		long task = meta.getTask();
		shellMsg.setTask(task);

		for (ByteString o: emissionProto.getContentsList()) {
		    shellMsg.addTuple(o);
		}
		return shellMsg;
	}

	@Override
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
    		tupleBuilder.addContents((ByteString)object);
    	}
        writeMessage(tupleBuilder.build());
	}

	@Override
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

	@Override
	public void writeTaskIds(List<Integer> taskIds) throws IOException {
		ShellMessages.TaskIds.Builder tasksProto = ShellMessages.TaskIds.newBuilder();
		for (int taskId : taskIds) {
			tasksProto.addTaskIds(taskId);
		}
        writeMessage(tasksProto.build());
	}

	private void writeMessage(Message msg) throws IOException {
        msg.writeDelimitedTo(processIn);
    }

	private Object readMessage(Parser parser) throws IOException {
        return parser.parseDelimitedFrom(processOut);
    }
}
