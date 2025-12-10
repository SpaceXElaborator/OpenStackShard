package com.rahman.shard.OpenStackShard.ui;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.openstack4j.api.OSClient.OSClientV3;
import org.openstack4j.model.image.v2.Image;
import org.openstack4j.openstack.OSFactory;

import com.rahman.arctic.shard.ShardProviderUICreation;
import com.rahman.arctic.shard.objects.providers.ProviderImage;
import com.rahman.arctic.shard.shards.UIField;

public class ObtainOS extends ShardProviderUICreation<ProviderImage> {

	@UIField(key = "osId", label = "Openstack Image")
	public CompletableFuture<List<ProviderImage>> returnResult() {
		return CompletableFuture.supplyAsync(() -> {
            List<ProviderImage> images = new ArrayList<>();
            List<? extends Image> osImages = OSFactory.clientFromToken(getClient(OSClientV3.class).getToken()).imagesV2().list();
            osImages.forEach(e -> {
                images.add(new ProviderImage(e.getId(), e.getName()));
            });
            return images;
        });
	}

}