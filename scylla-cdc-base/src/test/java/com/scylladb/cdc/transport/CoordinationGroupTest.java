package com.scylladb.cdc.transport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.HashSet;
import java.util.Set;

import org.junit.jupiter.api.Test;

public class CoordinationGroupTest {
    @Test
    public void copiesParticipantsAndExposesImmutableSet() {
        Set<String> participants = new HashSet<>(Set.of("one", "two"));
        CoordinationGroup<String, String> group =
                new CoordinationGroup<>("operation", "key", participants);

        participants.add("three");

        assertEquals(Set.of("one", "two"), group.getParticipants());
        assertThrows(UnsupportedOperationException.class,
                () -> group.getParticipants().add("three"));
    }

    @Test
    public void namespaceSeparatesGroupsWithSameKeyAndParticipants() {
        CoordinationGroup<String, String> first =
                new CoordinationGroup<>("first", "key", Set.of("participant"));
        CoordinationGroup<String, String> second =
                new CoordinationGroup<>("second", "key", Set.of("participant"));

        assertEquals(2, Set.of(first, second).size());
    }
}
