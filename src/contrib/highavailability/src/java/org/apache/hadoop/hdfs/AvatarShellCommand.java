/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;

import org.apache.commons.cli.OptionBuilder;

import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

public class AvatarShellCommand {

  private static String WRONG = " - wrong arguments: ";
  private static String CMD = "Command: ";

  private static List<String> safeModeActions = new ArrayList<String>();
  static {
    safeModeActions.add("enter");
    safeModeActions.add("leave");
    safeModeActions.add("get");
    safeModeActions.add("initqueues");
    safeModeActions.add("prepfailover");
    safeModeActions.add("get");
  }

  // commands
  static final String waitTxIdCommand = "waittxid";
  static final String failoverCommand = "failover";
  static final String prepfailoverCommand = "prepfailover";
  static final String metasaveCommand = "metasave";
  static final String safemodeCommand = "safemode";
  static final String setAvatarCommand = "setAvatar";
  static final String isInitializedCommand = "isInitialized";
  static final String shutdownAvatarCommand = "shutdownAvatar";
  static final String showAvatarCommand = "showAvatar";
  static final String saveNamespaceCommand = "saveNamespace";

  // helpers
  static final String zeroCommand = "zero";
  static final String oneCommand = "one";
  static final String serviceCommand = "service";
  static final String addressCommand = "address";

  // commands
  boolean isWaitTxIdCommand;
  boolean isFailoverCommand;
  boolean isPrepfailoverCommand;

  boolean isMetasaveCommand;
  String[] metasageArgs;

  boolean isSafemodeCommand;
  String[] safemodeArgs;

  boolean isSetAvatarCommand;
  String[] setAvatarArgs;

  boolean isIsInitializedCommand;
  boolean isShutdownAvatarCommand;
  boolean isShowAvatarCommand;

  boolean isSaveNamespaceCommand;
  String[] saveNamespaceArgs;

  // helpers
  boolean isZeroCommand;
  boolean isOneCommand;

  boolean isServiceCommand;
  String[] serviceArgs;

  boolean isAddressCommand;
  String[] addressArgs;

  List<String> getSaveNamespaceArgs() {
    List<String> args = new ArrayList<String>();
    if (saveNamespaceArgs != null) {
      for (String arg : saveNamespaceArgs) {
        args.add(arg);
      }
    }
    return args;
  }

  void throwException(String msg) {
    System.err.println(msg);
    throw new IllegalArgumentException(msg);
  }

  /**
   * For commands that directly obtain information from zk
   */
  void noZeroOrOneOrAddress(String command) {
    if (isZeroCommand || isOneCommand) {
      throwException(CMD + command + " (zero|one) should not be specified");
    }
    if (isAddressCommand) {
      throwException(CMD + command + " address should not be specified");
    }
  }

  /**
   * Check if either -zero -one or -address are specified.
   */
  void eitherZeroOrOneOrAddress(String command) {
    if (!isAddressCommand && !(isZeroCommand ^ isOneCommand)) {
      throwException(CMD + command + ": (zero|one) specified incorrectly");
    }
    if (isAddressCommand && (isZeroCommand || isOneCommand)) {
      throwException(CMD + command + ": cannot specify address with (zero|one)");
    }
  }

  private void assertArgsSize(String command, String[] args, int maxSize,
      int requiredSize) {
    if (args == null && requiredSize == 0) {
      return;
    }
    if (args.length < requiredSize || args.length > maxSize) {
      throwException(CMD + command + WRONG + Arrays.toString(args));
    }
  }

  boolean validate(CommandLine cmd) {
    int commands = 0;
    commands += isWaitTxIdCommand ? 1 : 0;
    commands += isFailoverCommand ? 1 : 0;
    commands += isPrepfailoverCommand ? 1 : 0;
    commands += isMetasaveCommand ? 1 : 0;
    commands += isSafemodeCommand ? 1 : 0;
    commands += isSetAvatarCommand ? 1 : 0;
    commands += isIsInitializedCommand ? 1 : 0;
    commands += isShutdownAvatarCommand ? 1 : 0;
    commands += isShowAvatarCommand ? 1 : 0;
    commands += isSaveNamespaceCommand ? 1 : 0;

    if (commands > 1) {
      throwException("More than one command specified");
    }

    if (isZeroCommand && isOneCommand) {
      throwException("Both zero and one specified");
    }

    if ((isServiceCommand || isZeroCommand || isOneCommand) && isAddressCommand) {
      throwException("Both service|zero|one and address specified");
    }

    // /////////////////////// complext commands

    // -waittxid
    if (isWaitTxIdCommand) {
      noZeroOrOneOrAddress(waitTxIdCommand);
      assertArgsSize(waitTxIdCommand, cmd.getOptionValues(waitTxIdCommand), 0,
          0);
    }

    // -failover
    if (isFailoverCommand) {
      noZeroOrOneOrAddress(failoverCommand);
      assertArgsSize(failoverCommand, cmd.getOptionValues(failoverCommand), 0,
          0);
    }

    // -prepfalover
    if (isPrepfailoverCommand) {
      noZeroOrOneOrAddress(prepfailoverCommand);
      assertArgsSize(prepfailoverCommand,
          cmd.getOptionValues(prepfailoverCommand), 0, 0);
    }

    // /////////////// per-avatar node commands

    // -showAvatar
    if (isShowAvatarCommand) {
      eitherZeroOrOneOrAddress(showAvatarCommand);
      assertArgsSize(showAvatarCommand, cmd.getOptionValues(showAvatarCommand),
          0, 0);
    }

    // -shutDownAvatar
    if (isShutdownAvatarCommand) {
      eitherZeroOrOneOrAddress(shutdownAvatarCommand);
      assertArgsSize(shutdownAvatarCommand,
          cmd.getOptionValues(shutdownAvatarCommand), 0, 0);
    }

    // -isInitialized
    if (isIsInitializedCommand) {
      eitherZeroOrOneOrAddress(isInitializedCommand);
      assertArgsSize(isInitializedCommand,
          cmd.getOptionValues(isInitializedCommand), 0, 0);
    }

    // -setAvatar
    if (isSetAvatarCommand) {
      eitherZeroOrOneOrAddress(setAvatarCommand);
      setAvatarArgs = cmd.getOptionValues(setAvatarCommand);
      assertArgsSize(setAvatarCommand, setAvatarArgs, 2, 1);
      if (setAvatarArgs != null) {
        boolean hasPrimary = false;
        for (String arg : setAvatarArgs) {
          if (!(arg.equals("primary") || arg.equals("force")))
            throwException(CMD + setAvatarCommand + WRONG
                + Arrays.toString(setAvatarArgs));
          if (arg.equals("primary"))
            hasPrimary = true;
        }
        if (!hasPrimary) {
          throwException(CMD + setAvatarCommand + WRONG
              + Arrays.toString(setAvatarArgs));
        }
      }
    }

    // -metasave
    if (isMetasaveCommand) {
      eitherZeroOrOneOrAddress(metasaveCommand);
      metasageArgs = cmd.getOptionValues(metasaveCommand);
      assertArgsSize(metasaveCommand, metasageArgs, 1, 1);
    }

    // -safemode
    if (isSafemodeCommand) {
      eitherZeroOrOneOrAddress(safemodeCommand);
      safemodeArgs = cmd.getOptionValues(safemodeCommand);
      assertArgsSize(safemodeCommand, safemodeArgs, 1, 1);
      for (String arg : safemodeArgs) {
        if (!safeModeActions.contains(arg))
          throwException(CMD + safemodeCommand + " - safemode action: " + arg
              + " unknown");
      }
    }

    // -saveNamespace
    if (isSaveNamespaceCommand) {
      eitherZeroOrOneOrAddress(saveNamespaceCommand);
      saveNamespaceArgs = cmd.getOptionValues(saveNamespaceCommand);
      assertArgsSize(saveNamespaceCommand, saveNamespaceArgs, 2, 0);
      if (saveNamespaceArgs != null) {
        for (String arg : saveNamespaceArgs) {
          if (!(arg.equals("uncompressed") || arg.equals("force")))
            throwException(CMD + saveNamespaceCommand + WRONG
                + Arrays.toString(saveNamespaceArgs));
        }
      }
    }

    // -service
    if (isServiceCommand) {
      serviceArgs = cmd.getOptionValues(serviceCommand);
      assertArgsSize(serviceCommand, serviceArgs, 1, 1);
    }

    // -address
    if (isAddressCommand) {
      addressArgs = cmd.getOptionValues(addressCommand);
      assertArgsSize(addressCommand, addressArgs, 1, 1);
      int i = addressArgs[0].indexOf(":");
      if (i < 1)
        throwException(addressCommand + ": wrong host:port pair");
      try {
        Integer.valueOf(addressArgs[0].substring(i + 1));
      } catch (Exception e) {
        throwException(addressCommand + ": wrong host:port pair");
      }
    }

    // -zero -one
    assertArgsSize(zeroCommand, cmd.getOptionValues(zeroCommand), 0, 0);
    assertArgsSize(oneCommand, cmd.getOptionValues(oneCommand), 0, 0);

    return true;
  }

  static AvatarShellCommand parseCommand(String[] argv) {
    CommandLine cmd = parseOptions(argv);
    if (cmd == null)
      return null;

    AvatarShellCommand asc = new AvatarShellCommand();

    asc.isWaitTxIdCommand = cmd.hasOption(waitTxIdCommand);
    asc.isFailoverCommand = cmd.hasOption(failoverCommand);
    asc.isPrepfailoverCommand = cmd.hasOption(prepfailoverCommand);
    asc.isMetasaveCommand = cmd.hasOption(metasaveCommand);
    asc.isSafemodeCommand = cmd.hasOption(safemodeCommand);
    asc.isSetAvatarCommand = cmd.hasOption(setAvatarCommand);
    asc.isIsInitializedCommand = cmd.hasOption(isInitializedCommand);
    asc.isShutdownAvatarCommand = cmd.hasOption(shutdownAvatarCommand);
    asc.isShowAvatarCommand = cmd.hasOption(showAvatarCommand);
    asc.isSaveNamespaceCommand = cmd.hasOption(saveNamespaceCommand);

    asc.isZeroCommand = cmd.hasOption(zeroCommand);
    asc.isOneCommand = cmd.hasOption(oneCommand);
    asc.isServiceCommand = cmd.hasOption(serviceCommand);
    asc.isAddressCommand = cmd.hasOption(addressCommand);

    try {
      asc.validate(cmd);
    } catch (Exception e) {
      return null;
    }
    return asc;
  }

  private static CommandLine parseOptions(String[] argv) {
    Options options = buildOptions();
    CommandLineParser parser = new PosixParser();
    try {
      return parser.parse(options, argv);
    } catch (ParseException e) {
      System.out.println("Error parsing command-line options");
      return null;
    }
  }

  /**
   * Build command-line options and descriptions
   */
  @SuppressWarnings("static-access")
  public static Options buildOptions() {
    Options options = new Options();

    // actual commands
    options.addOption(OptionBuilder.hasOptionalArgs().create(waitTxIdCommand));
    options.addOption(OptionBuilder.hasOptionalArgs().create(failoverCommand));
    options.addOption(OptionBuilder.hasOptionalArgs().create(
        prepfailoverCommand));

    options.addOption(OptionBuilder.hasOptionalArgs().create(metasaveCommand));
    options.addOption(OptionBuilder.hasOptionalArgs().create(safemodeCommand));
    options.addOption(OptionBuilder.hasOptionalArgs().create(
        saveNamespaceCommand));

    options.addOption(OptionBuilder.hasOptionalArgs().create(setAvatarCommand));
    options.addOption(OptionBuilder.hasOptionalArgs().create(
        isInitializedCommand));
    options.addOption(OptionBuilder.hasOptionalArgs().create(
        shutdownAvatarCommand));
    options
        .addOption(OptionBuilder.hasOptionalArgs().create(showAvatarCommand));

    // helper commands
    options.addOption(OptionBuilder.hasOptionalArgs().create(zeroCommand));
    options.addOption(OptionBuilder.hasOptionalArgs().create(oneCommand));

    options.addOption(OptionBuilder.hasOptionalArgs().create(serviceCommand));
    options.addOption(OptionBuilder.hasOptionalArgs().create(addressCommand));

    return options;
  }
}
