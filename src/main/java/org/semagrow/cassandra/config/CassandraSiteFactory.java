package org.semagrow.cassandra.config;

import org.semagrow.cassandra.CassandraSite;
import org.semagrow.selector.Site;
import org.semagrow.selector.SiteConfig;
import org.semagrow.selector.SiteFactory;

/**
 * Created by angel on 5/4/2016.
 */
public class CassandraSiteFactory implements SiteFactory {

    @Override
    public String getType() {
        return CassandraSiteConfig.TYPE;
    }

    @Override
    public SiteConfig getConfig() {
        return new CassandraSiteConfig();
    }

    @Override
    public Site getSite(SiteConfig config) {
        if (config instanceof CassandraSiteConfig) {
            CassandraSiteConfig cassandraSiteConfig = (CassandraSiteConfig)config;
            return new CassandraSite(cassandraSiteConfig.getEndpoint());
        }
        else
            throw new IllegalArgumentException("config is not of type CassandraSiteConfig");

    }

}
