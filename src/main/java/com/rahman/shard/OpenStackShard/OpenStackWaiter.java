package com.rahman.shard.OpenStackShard;

import org.openstack4j.api.OSClient.OSClientV3;
import org.openstack4j.model.compute.Server;
import org.openstack4j.model.network.Network;
import org.openstack4j.model.network.State;
import org.openstack4j.model.storage.block.Volume;

import com.rahman.arctic.shard.Waiter;
import com.rahman.arctic.shard.exceptions.ResourceErrorException;
import com.rahman.arctic.shard.exceptions.ResourceTimeoutException;
import com.rahman.arctic.shard.messaging.ConsoleMessage;
import com.rahman.arctic.shard.messaging.IcebergViewer;

public class OpenStackWaiter {

	/**
	 * Waits on a Volume to become available in OpenStack
	 * @param <R> Resource to be waited on
	 * @return Waiter of type Volume
	 */
	public static <T, R> Waiter<T, R> waitForVolumeAvailable() {
		return new Waiter<T, R>() {
			@Override
			public boolean waitUntilReady(T client, String re, R resource, int timeInSeconds, int pollingTimeInSeconds) throws ResourceTimeoutException, ResourceErrorException {
				Volume vol = (Volume) resource;
				OSClientV3 c = (OSClientV3)client;
	            long start = System.currentTimeMillis();
	            String startMessage = String.format("Creating Volume: %s [%s]...", vol.getName(), vol.getId());
	            IcebergViewer.sendConsoleBuildUpdate(re, new ConsoleMessage(startMessage));
	            while (vol.getStatus() != Volume.Status.AVAILABLE) {
	                if (System.currentTimeMillis() - start > timeInSeconds * 1000) {
	                	String msg = String.format("Volume %s [%s] Not Available After: %d Seconds", vol.getName(), vol.getId(), timeInSeconds);
	                	IcebergViewer.sendConsoleBuildUpdate(re, new ConsoleMessage(msg));
	                    throw new ResourceTimeoutException(msg);
	                }
	                if(vol.getStatus() == Volume.Status.ERROR) {
	                	throw new ResourceErrorException("Volume Entered An `ERROR` State");
	                }
	                try {
	                	String msg = String.format("Waiting for Volume %s [%s] To Become Available... (%ds) - %s", vol.getName(), vol.getId(), (int)((System.currentTimeMillis() - start)/1000), vol.getStatus().toString());
	                	IcebergViewer.sendConsoleBuildUpdate(re, new ConsoleMessage(msg));
	                	Thread.sleep(pollingTimeInSeconds * 1000);
	                } catch (InterruptedException e) {
	                    Thread.currentThread().interrupt();
	                }
	                vol = c.blockStorage().volumes().get(vol.getId());
	            }
	            String msg = String.format("Volume %s [%s] Was Built After (%d) Seconds", vol.getName(), vol.getId(), (int)((System.currentTimeMillis() - start)/1000));
	            IcebergViewer.sendConsoleBuildUpdate(re, new ConsoleMessage(msg));
	            return true;
			}
		};
	}
	
	/**
	 * Waits on a Server to become available in OpenStack
	 * @param <R> Resource to be waited on
	 * @return Waiter of type Volume
	 */
	public static <T, R> Waiter<T, R> waitForInstanceAvailable() {
	    return new Waiter<T, R>() {
	        @Override
	        public boolean waitUntilReady(T client, String re, R resource, int timeInSeconds, int pollingTimeInSeconds) throws ResourceTimeoutException, ResourceErrorException {
	            Server srv = (Server) resource;
	            OSClientV3 c = (OSClientV3)client;
	            long start = System.currentTimeMillis();
	            String startMessage = String.format("Creating Instance: %s [%s]...", srv.getName(), srv.getId());
	            IcebergViewer.sendConsoleBuildUpdate(re, new ConsoleMessage(startMessage));
	            while (srv.getStatus() != Server.Status.ACTIVE) {
	                if (System.currentTimeMillis() - start > timeInSeconds * 1000) {
	                	String msg = String.format("Instance %s [%s] Not Available After: %d Seconds", srv.getName(), srv.getId(), timeInSeconds);
	                	IcebergViewer.sendConsoleBuildUpdate(re, new ConsoleMessage(msg));
	                    throw new ResourceTimeoutException(msg);
	                }
	                if (srv.getStatus() == Server.Status.ERROR) {
	                    String errorMessage = "";
	                    if (srv.getFault() != null) {
	                        errorMessage = srv.getFault().getMessage();
	                    }
	                    throw new ResourceErrorException("Instance entered ERROR state: " + errorMessage);
	                }
	                try {
	                	String msg = String.format("Waiting for Instance %s [%s] To Become Available... (%ds)", srv.getName(), srv.getId(), (int)((System.currentTimeMillis() - start)/1000));
	                	IcebergViewer.sendConsoleBuildUpdate(re, new ConsoleMessage(msg));
	                    Thread.sleep(pollingTimeInSeconds * 1000);
	                } catch (InterruptedException e) {
	                    Thread.currentThread().interrupt();
	                }
	                srv = c.compute().servers().get(srv.getId());
	            }
	            String msg = String.format("Instance %s [%s] Was Built After (%d) Seconds", srv.getName(), srv.getId(), (int)((System.currentTimeMillis() - start)/1000));
	            IcebergViewer.sendConsoleBuildUpdate(re, new ConsoleMessage(msg));
	            return true;
	        }
	    };
	}
	
	/**
	 * Waits on a Network to become available in OpenStack
	 * @param <R> Resource to be waited on
	 * @return Waiter of type Network
	 */
	public static <T, R> Waiter<T, R> waitForNetworkUp() {
		return new Waiter<T, R>() {
			@Override
			public boolean waitUntilReady(T client, String re, R resource, int timeInSeconds, int pollingTimeInSeconds) throws ResourceTimeoutException, ResourceErrorException {
				Network net = (Network) resource;
				OSClientV3 c = (OSClientV3)client;
				long start = System.currentTimeMillis();
				while(net.getStatus() != State.ACTIVE) {
					if(System.currentTimeMillis() - start > timeInSeconds * 1000) {
						throw new ResourceTimeoutException("Network Not Availabe After " + String.valueOf(timeInSeconds) + " Seconds");
					}
					if(net.getStatus() == State.ERROR) {
						throw new ResourceErrorException("Network entered ERROR state");
					}
					try {
						Thread.sleep(pollingTimeInSeconds * 1000);
					} catch (InterruptedException e) {
						Thread.currentThread().interrupt();
					}
					net = c.networking().network().get(net.getId());
				}
				return true;
			}
		};
	}
	
}