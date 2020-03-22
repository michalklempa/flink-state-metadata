/*
 * Copyright 2020 Michal Klempa
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.michalklempa.flink.state.metadata;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.helper.HelpScreenException;
import net.sourceforge.argparse4j.impl.Arguments;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static ch.qos.logback.classic.Level.DEBUG;

public class Main {
    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    private static final ArgumentParser parser = ArgumentParsers.newFor("java -jar state-metadata.jar").build()
            .description("Flink State Metadata - manipulate Flink State _metadata file to enable snapshot moving to different location.\n");

    static {
        parser.addArgument("--debug")
                .type(Boolean.class)
                .setDefault(Boolean.FALSE)
                .required(false)
                .action(Arguments.storeTrue())
                .help("Default: false. Debug loglevel");
        parser.addArgument("--input.file")
                .type(String.class)
                .metavar("<location of _metadata file on local hard drive>")
                .required(true)
                .help("Input location of _metadata file on local hard drive. Example: ./flink/savepoints/savepoint-6337df-c8bb2d00a859/_metadata");
        parser.addArgument("--output.file")
                .type(String.class)
                .metavar("<location of _metadata file on local hard drive>")
                .required(true)
                .help("Output location of _metadata file on local hard drive. Example: ./different_bucket/savepoints/savepoint-6337df-c8bb2d00a859/_metadata");
        parser.addArgument("--input.uri")
                .type(String.class)
                .metavar("<savepoint uri>")
                .required(true)
                .help("Input savepoint URI in form <protocol>://<bucket name>/<path to savepoint>/_metadata. Example: s://flink/savepoints/savepoint-6337df-c8bb2d00a859");
        parser.addArgument("--output.uri")
                .type(String.class)
                .required(true)
                .metavar("<savepoint uri>")
                .help("Input savepoint URI in form <protocol>://<bucket name>/<path to savepoint>/_metadata. Example: s://different_bucket/savepoints/savepoint-6337df-c8bb2d00a859");
    }

    public static void main(String[] args) throws Exception {
        Namespace res;
        try {
            res = parser.parseArgs(args);
        } catch (HelpScreenException ex){
            return;
        } catch (ArgumentParserException ex) {
            parser.handleError(ex);
            throw ex;
        }

        if (res.getBoolean("debug")) {
            ((ch.qos.logback.classic.Logger) LoggerFactory.getLogger("com.michalklempa")).setLevel(DEBUG);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Provided configuration:");
            List<Map.Entry<String, Object>> list = new ArrayList<>();
            for (Map.Entry<String, Object> entry : res.getAttrs().entrySet()) {
                list.add(entry);
            }
            list.sort((o1, o2) -> o1.getKey().compareTo(o2.getKey()));
            for (Map.Entry<String, Object> entry : list) {
                LOG.debug("{}: {}", entry.getKey(), entry.getValue());
            }
        }

        String outputFilename = res.getString("output.file");
        String inputFilename = res.getString("input.file");
        String outputUri = res.getString("output.uri");
        String inputUri = res.getString("input.uri");

        Move move = new Move.Default();
        new Save.Default().save(move.move(new Load.Default().load(inputFilename), inputUri, outputUri), outputFilename);
    }
}

