package com.rahman.shard.OpenStackShard.ui;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.openstack4j.api.OSClient.OSClientV3;
import org.openstack4j.model.compute.Flavor;
import org.openstack4j.openstack.OSFactory;

import com.rahman.arctic.shard.ShardProviderUICreation;
import com.rahman.arctic.shard.objects.providers.ProviderFlavor;
import com.rahman.arctic.shard.shards.UIField;

public class ObtainFlavors extends ShardProviderUICreation<ProviderFlavor> {

	@UIField(key = "flavorId", label = "Openstack Flavor")
	public CompletableFuture<List<ProviderFlavor>> returnResult() {
		return CompletableFuture.supplyAsync(() -> {
            List<ProviderFlavor> images = new ArrayList<>();
            List<? extends Flavor> osImages = OSFactory.clientFromToken(getClient(OSClientV3.class).getToken()).compute().flavors().list();
            osImages.forEach(e -> {
                images.add(new ProviderFlavor(e.getId(), e.getName()));
            });
            return images;
        });
	}
	
}
