package org.vortex;

import java.util.HashMap;
import java.util.Map;

public class Settings extends HashMap<String, String> {
    public Settings() {
    }

    public static Settings defaults() {
        Settings settings = new Settings();
        settings.put("elasticsearch.db.url", "localhost:9300");
        settings.put("mongo.db.url", "mongodb://localhost:27017");
        settings.put("mongo.db.name", "vortex");
        settings.put("http.notification.url", "http://localhost:8002/notify");
        settings.put("orientdb.db.url", "plocal:localhost");
        settings.put("orientdb.db.name", "vortex");
        settings.put("orientdb.dbPool.min", "5");
        settings.put("orientdb.dbPool.max", "20");
        return settings;
    }

    public String dbUrl(String target) {
        String dbUrlKey = target + ".db.url";
        return get(dbUrlKey);
    }

    public String dbName(String target) {
        String dbName = target + ".db.name";
        return get(dbName);
    }

    public String getOrDefault(String key, String defaultVal) {
        return containsKey(key) ? get(key) : defaultVal;
    }

    public static Settings overrideDefaults(Settings overriddenSettings) {
        Settings toBeOverridden = defaults();
        return override(toBeOverridden, overriddenSettings);
    }

    public static Settings override(Settings toBeOverridden, Settings overriddenSettings) {
        for (Map.Entry<String, String> keyValue : overriddenSettings.entrySet()) {
            toBeOverridden.put(keyValue.getKey(), keyValue.getValue());
        }
        return toBeOverridden;
    }

    /**
     * @param hosts comma separated hosts E.g.: host1.mongodb.co,host2.mongodb.co:27027
     */
    public void mongoHosts(String hosts) {
        this.put("mongo.db.url", String.format("mongodb://%s", hosts));
    }


    public String httpNotificationUrl() {
        return get("http.notification.url");
    }

    public String notificationUrl(String stateCaptureMethod) {
        return get(String.format("%s.notification.url", stateCaptureMethod));
    }
}
