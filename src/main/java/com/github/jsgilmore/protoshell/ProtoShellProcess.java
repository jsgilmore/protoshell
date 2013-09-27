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

import backtype.storm.task.TopologyContext;
import java.io.InputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Set;

import com.google.protobuf.Message;
import com.google.protobuf.Parser;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

public class ProtoShellProcess {
    public static Logger LOG = Logger.getLogger(ProtoShellProcess.class);
    private DataOutputStream processIn;
    private InputStream processOut;
    private InputStream processErrorStream;
    private Process _subprocess;
    private String[] command;

    public ProtoShellProcess(String[] command) {
        this.command = command;
    }

    public Number launch(Map conf, TopologyContext context) throws IOException {
        ProcessBuilder builder = new ProcessBuilder(command);
        builder.directory(new File(context.getCodeDir()));
        _subprocess = builder.start();

        processIn = new DataOutputStream(_subprocess.getOutputStream());
        processOut = _subprocess.getInputStream();
        processErrorStream = _subprocess.getErrorStream();
        
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

    public void destroy() {
        _subprocess.destroy();
    }

    public void writeMessage(Message msg) throws IOException {
        msg.writeDelimitedTo(processIn);
        processIn.flush();
    }
    
    public Object readMessage(Parser parser) throws IOException {
        return parser.parseDelimitedFrom(processOut);
    }

    public String getErrorsString() {
        if(processErrorStream!=null) {
            try {
                return IOUtils.toString(processErrorStream);
            } catch(IOException e) {
                return "(Unable to capture error stream)";
            }
        } else {
            return "";
        }
    }

    public void drainErrorStream()
    {
        try {
            while (processErrorStream.available() > 0)
            {
                int bufferSize = processErrorStream.available();
                byte[] errorReadingBuffer =  new byte[bufferSize];

                processErrorStream.read(errorReadingBuffer, 0, bufferSize);

                LOG.info("Got error from shell process: " + new String(errorReadingBuffer));
            }
        } catch(Exception e) {
        }
    }
}
