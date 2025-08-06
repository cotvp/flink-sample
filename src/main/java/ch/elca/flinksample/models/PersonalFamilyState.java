package ch.elca.flinksample.models;

import java.util.Set;

public record PersonalFamilyState(String personId, String familyId, long offset, Set<String> familyMemberRefs) {
}
