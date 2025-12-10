package com.rahman.shard.OpenStackShard;

import java.util.ArrayList;
import java.util.List;

import org.openstack4j.api.Builders;
import org.openstack4j.api.OSClient.OSClientV3;
import org.openstack4j.model.common.Identifier;
import org.openstack4j.model.compute.BDMDestType;
import org.openstack4j.model.compute.BDMSourceType;
import org.openstack4j.model.compute.Server;
import org.openstack4j.model.compute.builder.ServerCreateBuilder;
import org.openstack4j.model.network.AttachInterfaceType;
import org.openstack4j.model.network.IPVersionType;
import org.openstack4j.model.network.Network;
import org.openstack4j.model.network.Router;
import org.openstack4j.model.network.SecurityGroup;
import org.openstack4j.model.network.SecurityGroupRule;
import org.openstack4j.model.network.Subnet;
import org.openstack4j.model.network.builder.RouterBuilder;
import org.openstack4j.model.storage.block.Volume;
import org.openstack4j.openstack.OSFactory;

import com.rahman.arctic.shard.ShardProviderTmpl;
import com.rahman.arctic.shard.Waiter;
import com.rahman.arctic.shard.exceptions.ResourceErrorException;
import com.rahman.arctic.shard.exceptions.ResourceTimeoutException;
import com.rahman.arctic.shard.objects.ArcticHostSO;
import com.rahman.arctic.shard.objects.ArcticNetworkSO;
import com.rahman.arctic.shard.objects.ArcticRouterSO;
import com.rahman.arctic.shard.objects.ArcticSecurityGroupRuleSO;
import com.rahman.arctic.shard.objects.ArcticSecurityGroupSO;
import com.rahman.arctic.shard.objects.ArcticTask;
import com.rahman.arctic.shard.objects.ArcticVolumeSO;
import com.rahman.arctic.shard.util.UserDataHelper;
import com.rahman.shard.OpenStackShard.ui.ObtainFlavors;
import com.rahman.shard.OpenStackShard.ui.ObtainOS;

public class OpenStackShard extends ShardProviderTmpl<OSClientV3> {

	@Override
	public String getDomain() {
		return "openstack";
	}
	
	public void pluginEnabled() {
		registerUICreation(new ObtainOS());
		registerUICreation(new ObtainFlavors());
	}
	
	@Override
	public OSClientV3 createClient() {
		System.out.println("Attempting to Create OpenStack Client...");
		
		System.out.println("Attempting to load properties...");
		String endpoint = getProperties().getPropertyValue("endpoint");
		String username = getProperties().getPropertyValue("username");
		String password = getProperties().getPropertyValue("password");
		String projectId = getProperties().getPropertyValue("projectId");
		
		if(endpoint == null || username == null || password == null || projectId == null) {
			System.out.println("Required configuration details do not exists. Please add and re-run.");
			System.exit(1);
			return null;
		}
		
		String domain = getProperties().getPropertyValue("domain");
		
		if(domain == null) {
			domain = "Default";
		}
		
		System.out.println("Connecting with the following options:");
		System.out.println("\tEndpoint: " + endpoint);
		System.out.println("\tUsername: " + username);
		System.out.println("\tPassword: *****");
		System.out.println("\tProjectID: " + projectId);
		System.out.println("\tDomain: " + domain);
		OSClientV3 mainOSC = OSFactory.builderV3()
				.endpoint(endpoint)
				.credentials(username, password, Identifier.byName(domain))
				.scopeToProject(Identifier.byId(projectId))
				.authenticate();
		
		if(mainOSC != null) {
			System.out.println("OpenStack Client Successfully Loaded");
		}
		
		return mainOSC;
	}

	@Override
	protected ArcticTask<OSClientV3, Server> buildHost(ArcticHostSO ah) {
		// Create the lists that will hold the dependencies needed further into the method
		List<ArcticTask<OSClientV3, Volume>> volumes = new ArrayList<>();
		List<ArcticTask<OSClientV3, Network>> networks = new ArrayList<>();
		List<ArcticTask<OSClientV3, ?>> depends = new ArrayList<>();
		
		// Grab all networks and volumes from ArcticHost and add
		// 		them into the lists above
		ah.getNetworks().forEach(e -> {
			networks.add(getTypedTask(getNetworkTasks(), e));
			depends.add(getNetworkTasks().get(e));
		});
		
		ah.getVolumes().forEach(e -> {
			volumes.add(getTypedTask(getVolumeTasks(), e));
			depends.add(getVolumeTasks().get(e));
		});
		
		// Create the ArcticTask<Client, Resource>
		ArcticTask<OSClientV3, Server> server = new ArcticTask<OSClientV3, Server>(10, getClient(), depends) {
			
			// Actual action of building the Server following OSClientV3 Library
			public Server action() {
				ServerCreateBuilder scb = Builders.server();
				
				if(ah.getOsType().equalsIgnoreCase("linux")) {
					scb.userData(UserDataHelper.createBasicLinuxUserData(ah.getDefaultUser(), ah.getDefaultPassword()));
				}
				
				scb.configDrive(true);
				scb.name(ah.getName());
				scb.flavor(ah.getFlavor());
				scb.image(ah.getImageId());
				for(ArcticTask<OSClientV3, Volume> vol : volumes) {
					scb.blockDevice(Builders.blockDeviceMapping()
							.uuid(vol.getResource().getId())
							.bootIndex(0)
							.destinationType(BDMDestType.VOLUME)
							.sourceType(BDMSourceType.VOLUME)
							.deleteOnTermination(true)
							.build());
				}
				List<String> networkIds = new ArrayList<>();
				for(ArcticTask<OSClientV3, Network> net : networks) {
					Network netObj = net.getResource();
					networkIds.add(netObj.getId());
				}
				scb.networks(networkIds);
				Server s = OSFactory.clientFromToken(getClient().getToken()).compute().servers().boot(scb.build());
				return s;
			}
			
			// Use the OpenStackWaiter class to wait or error out the building of the Server
			public void waitMethod(Server s) {
				Waiter<OSClientV3, Server> serverWaiter = OpenStackWaiter.waitForInstanceAvailable();
				try {
					serverWaiter.waitUntilReady(OSFactory.clientFromToken(getClient().getToken()), ah.getRangeId(), s, 5000, 10);
				} catch (ResourceTimeoutException e) {
					e.printStackTrace();
				} catch (ResourceErrorException e) {
					e.printStackTrace();
				}
			}
		};
		
		// Return the ArcticTask
		return server;
	}
	
	@Override
	protected ArcticTask<OSClientV3, Network> buildNetwork(ArcticNetworkSO an) {
		ArcticTask<OSClientV3, Network> net =  new ArcticTask<OSClientV3, Network>(0, getClient()) {
			public Network action() {
				Network netObj = OSFactory.clientFromToken(getClient().getToken()).networking().network().create(Builders.network()
						.name(an.getName())
						.adminStateUp(true)
						.build());
				Waiter<OSClientV3, Network> netWaiter = OpenStackWaiter.waitForNetworkUp();
				try {
					netWaiter.waitUntilReady(OSFactory.clientFromToken(getClient().getToken()), an.getRangeId(), netObj, 3000, 10);
				} catch (ResourceTimeoutException | ResourceErrorException e1) {
					e1.printStackTrace();
				}
				Subnet s = getClient().networking().subnet().create(Builders.subnet()
						.name(an.getName() + "-Subnet")
						.networkId(netObj.getId())
						.enableDHCP(true)
						.addPool(an.getIpRangeStart(), an.getIpRangeEnd())
						.ipVersion(IPVersionType.V4)
						.cidr(an.getIpCidr())
						.gateway(an.getIpGateway())
						.build());
				netObj.getSubnets().add(s.getId());
				return netObj;
//				setResource(netObj);
			}

			@Override
			public void waitMethod(Network resource) {
				// TODO: Fix this, in this setup, the wait needs to be done else where, maybe make subnets and networks
					// Separate objects?
				return;
			}
		};
		
		return net;
	}

	@Override
	protected ArcticTask<OSClientV3, SecurityGroup> buildSecurityGroup(ArcticSecurityGroupSO asg) {
		ArcticTask<OSClientV3, SecurityGroup> secGroup = new ArcticTask<OSClientV3, SecurityGroup>(4, getClient()) {
			public SecurityGroup action() {
				SecurityGroup sg = OSFactory.clientFromToken(getClient().getToken()).networking().securitygroup().create(Builders.securityGroup()
						.name(asg.getName())
						.description(asg.getDescription())
						.build());
//				setResource(sg);
				return sg;
			}

			@Override
			public void waitMethod(SecurityGroup resource) {
				// TODO: Again, No Waiting Needed
				return;
			}
		};
		return secGroup;
	}

	@Override
	protected ArcticTask<OSClientV3, Router> buildRouter(ArcticRouterSO ar) {
		List<ArcticTask<OSClientV3, Network>> networks = new ArrayList<>();
		List<ArcticTask<OSClientV3, ?>> depends = new ArrayList<>();
		
		ar.getConnectedNetworkNames().forEach(e -> {
			networks.add(getTypedTask(getNetworkTasks(), e));
			depends.add(getNetworkTasks().get(e));
		});
		
		ArcticTask<OSClientV3, Router> router = new ArcticTask<OSClientV3, Router>(1, getClient(), depends) {
			public Router action() {
				OSClientV3 client = OSFactory.clientFromToken(getClient().getToken());
				
				RouterBuilder rb = Builders.router();
				rb.adminStateUp(true);
				rb.clearExternalGateway();
				rb.name(ar.getName());
				
				Router r = client.networking().router().create(rb.build());
				
				OSFactory.clientFromToken(getClient().getToken()).networking().router().attachInterface(ar.getName(), null, ar.getName());
				for(ArcticTask<OSClientV3, Network> net : networks) {
					client.networking().router().attachInterface(r.getId(), AttachInterfaceType.SUBNET, net.getResource().getSubnets().get(0));
				}
				
				return r;
			}

			@Override
			public void waitMethod(Router resource) {
				// TODO: NO wait is needed for this one... how do I make this better?
				return;
			}
		};
		
		return router;
	}

	@Override
	protected ArcticTask<OSClientV3, Volume> buildVolume(ArcticVolumeSO av) {
		ArcticTask<OSClientV3, Volume> vol = new ArcticTask<OSClientV3, Volume>(2, getClient()) {
			public Volume action() {
				Volume v = OSFactory.clientFromToken(getClient().getToken()).blockStorage().volumes().create(Builders.volume()
						.name(av.getName())
						.description(av.getDescription())
						.size(av.getSize())
						.imageRef(av.getImageId())
						.bootable(av.isBootable())
						.build());
				return v;
//				setResource(v);
			}

			@Override
			public void waitMethod(Volume resource) {
				Waiter<OSClientV3, Volume> waiter = OpenStackWaiter.waitForVolumeAvailable();
				try {
					waiter.waitUntilReady(OSFactory.clientFromToken(getClient().getToken()), av.getRangeId(), resource, 3000, 10);
				} catch (ResourceTimeoutException e) {
					e.printStackTrace();
				} catch (ResourceErrorException e) {
					e.printStackTrace();
				}
			}
		};
		return vol;
	}

	@Override
	protected ArcticTask<OSClientV3, SecurityGroupRule> buildSecurityGroupRule(ArcticSecurityGroupRuleSO asgr) {
		@SuppressWarnings("unchecked")
		ArcticTask<OSClientV3, SecurityGroup> group = (ArcticTask<OSClientV3, SecurityGroup>) getSecurityGroupTasks().get(asgr.getSecGroup());
		
		ArcticTask<OSClientV3, SecurityGroupRule> rule = new ArcticTask<OSClientV3, SecurityGroupRule>(5, getClient(), List.of(group)) {
			public SecurityGroupRule action() {
				//String startMessage = String.format("Creating Security Rule: %s %s %s-%s", dir, protocol, String.valueOf(r1), String.valueOf(r2));
				//IcebergViewer.sendConsoleBuildUpdate(re, new ConsoleMessage(startMessage));
				SecurityGroupRule sgr = OSFactory.clientFromToken(getClient().getToken()).networking().securityrule().create(
						Builders.securityGroupRule()
						.securityGroupId(group.getResource().getId())
						.direction(asgr.getDirection())
						.ethertype(asgr.getEth())
						.protocol(asgr.getProtocol())
						.portRangeMin(asgr.getStartPortRange())
						.portRangeMax(asgr.getEndPortRange())
						.build()
					);
//				setResource(sgr);
				//String endMessage = String.format("Security Rule Done: %s %s %s-%s", dir, protocol, String.valueOf(r1), String.valueOf(r2));
				//IcebergViewer.sendConsoleBuildUpdate(re, new ConsoleMessage(endMessage));
				return sgr;
			}

			@Override
			public void waitMethod(SecurityGroupRule resource) {
				// TODO: Again, No waiting needed
				return;
			}
		};
		return rule;
	}

//	@Override
//	public CompletableFuture<List<ProviderImage>> obtainOS() {
//		return CompletableFuture.supplyAsync(() -> {
//            List<ProviderImage> images = new ArrayList<>();
//            List<? extends Image> osImages = OSFactory.clientFromToken(getClient().getToken()).imagesV2().list();
//            osImages.forEach(e -> {
//                images.add(new ProviderImage(e.getId(), e.getName()));
//            });
//            return images;
//        });
//	}
//
//	@Override
//	public CompletableFuture<List<ProviderFlavor>> obtainFlavors() {
//		return CompletableFuture.supplyAsync(() -> {
//            List<ProviderFlavor> images = new ArrayList<>();
//            List<? extends Flavor> osImages = OSFactory.clientFromToken(getClient().getToken()).compute().flavors().list();
//            osImages.forEach(e -> {
//                images.add(new ProviderFlavor(e.getId(), e.getName()));
//            });
//            return images;
//        });
//	}

}