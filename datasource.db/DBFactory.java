package com.wayz.lbi.industry.datasource.db;

import java.util.EnumMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import com.wayz.lbi.industry.datasource.db.impl.Es;
import com.wayz.lbi.industry.datasource.db.impl.Fake;
import com.wayz.lbi.industry.datasource.db.impl.MySQL;


public final class DBFactory {

    private static final Map<Type, Supplier<DB>> DBS = new EnumMap<>(Type.class);

    // register the db here
    static {
        DBS.put(Type.MYSQL, MySQL::new);
        DBS.put(Type.ES, Es::new);
        DBS.put(Type.FAKE, Fake::getInstance);
    }

    public static Optional<DB> get(Type type) {
        return Optional.ofNullable(DBS.getOrDefault(type, () -> null).get());
    }
}
