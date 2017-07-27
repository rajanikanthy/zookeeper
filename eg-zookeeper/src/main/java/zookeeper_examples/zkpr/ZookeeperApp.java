package zookeeper_examples.zkpr;

import java.io.IOException;
import java.io.PrintStream;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeper.States;
import org.apache.zookeeper.data.Stat;

public class ZookeeperApp implements Watcher {
	
	private ZooKeeper zk;
	private static Integer mutex;
	
	public void connect(String address) throws KeeperException, IOException {
		if (address != null && !address.isEmpty()) {
			zk = new ZooKeeper(address, 5000 , this);
			mutex = new Integer(-1);
			System.out.println("Finished starting ZK : " + zk);
		}
	}
	
	public void greet(String message) {
		try {
			Stat stat = zk.exists("/lock", false);
			if (stat == null) {
				zk.create("/lock", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
				System.out.println(message);
			}
		} catch (KeeperException kexcep) {
			if (kexcep.code() == Code.NODEEXISTS) {
				System.out.println("Node already exists");
			} else {
				kexcep.printStackTrace();
			}
		} catch (InterruptedException iexcep) {
			System.out.println(new PrintStream(System.out));
		} finally {
			try {
				zk.close();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	public static void main(String[] args) {
		ZookeeperApp zkApp = new ZookeeperApp();
		try {
			zkApp.connect("127.0.0.1:2181");
			Thread.sleep(5 * 60 * 1000);
		} catch (Exception excep) {
			excep.printStackTrace();
		} 
	}

	@Override
	public void process(WatchedEvent event) {
		System.out.println(event.getState());
		synchronized(mutex) {
				mutex.notify();
				greet("Hello World");
		}
		
	}

}
