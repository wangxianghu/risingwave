#Hadoop Related Options
wget "https://archive.apache.org/dist/hadoop/core/hadoop-2.7.3/hadoop-2.7.3.tar.gz"
tar -zxvf hadoop-2.7.3.tar.gz -C ~/
mv ~/hadoop-2.7.3 ~/hadoop

echo export HADOOP_HOME=/home/ubuntu/hadoop >> ~/.bashrc
echo export PATH=/home/ubuntu/hadoop/bin:/home/ubuntu/hadoop/sbin:$PATH >> ~/.bashrc
echo export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64 >> ~/.bashrc
echo export LD_LIBRARY_PATH=${JAVA_HOME}/lib/server:${LD_LIBRARY_PATH} >> ~/.bashrc
source ~/.bashrc

echo export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64 >> /home/ubuntu/hadoop/etc/hadoop/yarn-env.sh
echo export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64 >> /home/ubuntu/hadoop/etc/hadoop/hadoop-env.sh



<< com
# install java 11
sudo apt update
sudo apt install openjdk-11-jdk -y
java -version; javac -version



# test ssh localhost
ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
chmod 0600 ~/.ssh/authorized_keys
ssh localhost

# Forward port
ssh -L 9870:localhost:9870 ubuntu@54.169.83.184

# core-site.xml:

<configuration>
    <property>
        <name>hadoop.tmp.dir</name>
        <value>file:/home/ubuntu/hadoop/tmp</value>
        <description>Abase for other temporary directories.</description>
    </property>
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://localhost:9000</value>
    </property>
</configuration>


# hdfs-site.xml:

<configuration>
    <property>
        <name>dfs.replication</name>
        <value>1</value>
    </property>
    <property>
        <name>dfs.permissions</name>
        <value>false</value>    
    </property>
    <property>
        <name>dfs.namenode.name.dir</name>
        <value>file:/home/ubuntu/hadoop/tmp/dfs/name</value>
    </property>
    <property>
        <name>dfs.datanode.data.dir</name>
        <value>file:/home/ubuntu/hadoop/tmp/dfs/data</value>
    </property>
    <property>
        <name>dfs.namenode.http-address</name>
        <value>localhost:9870</value>
    </property>
    <property>
        <name>dfs.secondary.http.address</name>
        <value>localhost:9100</value>
    </property>
</configuration>


