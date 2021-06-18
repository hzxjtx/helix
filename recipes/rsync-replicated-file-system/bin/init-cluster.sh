#!/bin/sh
set -x
# ----------------------------------------------------------------------------
#  Copyright 2001-2006 The Apache Software Foundation.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
# ----------------------------------------------------------------------------

#   Copyright (c) 2001-2002 The Apache Software Foundation.  All rights
#   reserved.

BASEDIR=`dirname $0`/..
BASEDIR=`(cd "$BASEDIR"; pwd)`



# OS specific support.  $var _must_ be set to either true or false.
cygwin=false;
darwin=false;
case "`uname`" in
  CYGWIN*) cygwin=true ;;
  Darwin*) darwin=true
           if [ -z "$JAVA_VERSION" ] ; then
             JAVA_VERSION="CurrentJDK"
           else
             echo "Using Java version: $JAVA_VERSION"
           fi
           if [ -z "$JAVA_HOME" ] ; then
             JAVA_HOME=/System/Library/Frameworks/JavaVM.framework/Versions/${JAVA_VERSION}/Home
           fi
           ;;
esac

if [ -z "$JAVA_HOME" ] ; then
  if [ -r /etc/gentoo-release ] ; then
    JAVA_HOME=`java-config --jre-home`
  fi
fi

# For Cygwin, ensure paths are in UNIX format before anything is touched
if $cygwin ; then
  [ -n "$JAVA_HOME" ] && JAVA_HOME=`cygpath --unix "$JAVA_HOME"`
  [ -n "$CLASSPATH" ] && CLASSPATH=`cygpath --path --unix "$CLASSPATH"`
fi

# If a specific java binary isn't specified search for the standard 'java' binary
if [ -z "$JAVACMD" ] ; then
  if [ -n "$JAVA_HOME"  ] ; then
    if [ -x "$JAVA_HOME/jre/sh/java" ] ; then
      # IBM's JDK on AIX uses strange locations for the executables
      JAVACMD="$JAVA_HOME/jre/sh/java"
    else
      JAVACMD="$JAVA_HOME/bin/java"
    fi
  else
    JAVACMD=`which java`
  fi
fi

if [ ! -x "$JAVACMD" ] ; then
  echo "Error: JAVA_HOME is not defined correctly."
  echo "  We cannot execute $JAVACMD"
  exit 1
fi

if [ -z "$REPO" ]
then
  REPO="$BASEDIR"/repo
fi

CLASSPATH=$CLASSPATH_PREFIX:"$BASEDIR"/conf:"$REPO"/org/slf4j/slf4j-api/1.7.25/slf4j-api-1.7.25.jar:"$REPO"/org/slf4j/slf4j-log4j12/1.7.14/slf4j-log4j12-1.7.14-sources.jar:"$REPO"/log4j/log4j/1.2.17/log4j-1.2.17.jar:"$REPO"/org/testng/testng/6.0.1/testng-6.0.1.jar:"$REPO"/org/beanshell/bsh/2.0b4/bsh-2.0b4.jar:"$REPO"/com/beust/jcommander/1.12/jcommander-1.12.jar:"$REPO"/org/yaml/snakeyaml/1.6/snakeyaml-1.6.jar:"$REPO"/org/apache/helix/helix-core/1.0.1/helix-core-1.0.1.jar:"$REPO"/org/apache/helix/helix-common/1.0.1/helix-common-1.0.1.jar:"$REPO"/org/apache/helix/metrics-common/1.0.1/metrics-common-1.0.1.jar:"$REPO"/org/apache/helix/zookeeper-api/1.0.1/zookeeper-api-1.0.1.jar:"$REPO"/org/apache/helix/metadata-store-directory-common/1.0.1/metadata-store-directory-common-1.0.1.jar:"$REPO"/com/fasterxml/jackson/core/jackson-databind/2.10.2/jackson-databind-2.10.2.jar:"$REPO"/com/fasterxml/jackson/core/jackson-annotations/2.10.2/jackson-annotations-2.10.2.jar:"$REPO"/com/fasterxml/jackson/core/jackson-core/2.10.2/jackson-core-2.10.2.jar:"$REPO"/org/apache/commons/commons-lang3/3.8.1/commons-lang3-3.8.1.jar:"$REPO"/org/apache/zookeeper/zookeeper/3.4.13/zookeeper-3.4.13.jar:"$REPO"/jline/jline/0.9.94/jline-0.9.94.jar:"$REPO"/org/apache/yetus/audience-annotations/0.5.0/audience-annotations-0.5.0.jar:"$REPO"/io/netty/netty/3.10.6.Final/netty-3.10.6.Final.jar:"$REPO"/org/slf4j/slf4j-log4j12/1.7.14/slf4j-log4j12-1.7.14.jar:"$REPO"/org/codehaus/jackson/jackson-core-asl/1.8.5/jackson-core-asl-1.8.5.jar:"$REPO"/org/codehaus/jackson/jackson-mapper-asl/1.8.5/jackson-mapper-asl-1.8.5.jar:"$REPO"/commons-io/commons-io/1.4/commons-io-1.4.jar:"$REPO"/commons-cli/commons-cli/1.2/commons-cli-1.2.jar:"$REPO"/org/apache/commons/commons-math/2.1/commons-math-2.1.jar:"$REPO"/org/apache/commons/commons-math3/3.6.1/commons-math3-3.6.1.jar:"$REPO"/commons-codec/commons-codec/1.6/commons-codec-1.6.jar:"$REPO"/com/google/guava/guava/15.0/guava-15.0.jar:"$REPO"/io/dropwizard/metrics/metrics-core/3.2.3/metrics-core-3.2.3.jar:"$REPO"/org/apache/httpcomponents/httpclient/4.5.8/httpclient-4.5.8.jar:"$REPO"/org/apache/httpcomponents/httpcore/4.4.11/httpcore-4.4.11.jar:"$REPO"/commons-logging/commons-logging/1.2/commons-logging-1.2.jar:"$REPO"/org/apache/commons/commons-jci-fam/1.0/commons-jci-fam-1.0.jar:"$REPO"/commons-logging/commons-logging-api/1.1/commons-logging-api-1.1.jar:"$REPO"/org/apache/helix/recipes/rsync-replicated-file-system/1.0.1/rsync-replicated-file-system-1.0.1.jar
EXTRA_JVM_ARGUMENTS="-Xms512m -Xmx512m"

# For Cygwin, switch paths to Windows format before running java
if $cygwin; then
  [ -n "$CLASSPATH" ] && CLASSPATH=`cygpath --path --windows "$CLASSPATH"`
  [ -n "$JAVA_HOME" ] && JAVA_HOME=`cygpath --path --windows "$JAVA_HOME"`
  [ -n "$HOME" ] && HOME=`cygpath --path --windows "$HOME"`
  [ -n "$BASEDIR" ] && BASEDIR=`cygpath --path --windows "$BASEDIR"`
  [ -n "$REPO" ] && REPO=`cygpath --path --windows "$REPO"`
fi

exec "$JAVACMD" $JAVA_OPTS \
  $EXTRA_JVM_ARGUMENTS \
  -classpath "$CLASSPATH" \
  -Dapp.name="quickdemo" \
  -Dapp.pid="$$" \
  -Dapp.repo="$REPO" \
  -Dbasedir="$BASEDIR" \
  org.apache.helix.filestore.SetupCluster \
  172.20.3.83:2181  \
  3
