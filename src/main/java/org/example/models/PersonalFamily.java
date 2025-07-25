package org.example.models;

import java.util.Set;

public record PersonalFamily(String personId, String familyId, long offset, Set<String> familyMemberRefs) {
}
