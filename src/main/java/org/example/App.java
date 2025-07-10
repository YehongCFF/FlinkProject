package org.example;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import java.util.HashSet;
import java.util.Set;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        System.out.println( "Hello World!" );
        Set<HostAndPort> nodes = new HashSet<>();
        nodes.add(new HostAndPort("spdb01", 6399));
        nodes.add(new HostAndPort("spdb02", 6399));
        nodes.add(new HostAndPort("spdb03", 6399));

        JedisCluster jedisCluster = new JedisCluster(nodes,"default","123456");
    }
}
