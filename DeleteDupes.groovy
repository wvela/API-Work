@ResponseBody
@PostMapping("/tools/deleteduplicates/{paid}")
ResponseEntity<?> dedupeProgramAiring(@PathVariable Integer paid, @RequestParam(value = "publish", defaultValue = "false") Boolean publish) {

	loccalUtil.processProgramAiringForDuplicates(paid, publish);
	return ResponseEntity.status(HttpStatus.OK).body("Duplicate airings have been deleted from PAid: " + paid);
}



public String processProgramAiringForDuplicates(Integer paid, Boolean publish) {
        Integer totalDeletionCount = 0;
        try {
            MDC.put("airingKey", "delete-duplicate-" + paid );

            List<DupeAdAiring> airings = loccalService.getOccurrencesForDedupe(paid);

            if (airings == null || airings.size() == 0) {
                LOGGER.debug("ZERO unlocked PA airings for paid: {}", paid);
            } else {
                LOGGER.debug("PAid has {} airings", airings.size());
                totalDeletionCount = findAndDeleteDupes(paid, airings, publish);
            }
        }
        catch (Exception ex) {
            LOGGER.error("Exception caught deleting duplicates for program airing ID: " + paid, ex);
        }
        finally {
            MDC.clear();
        }

        return "PAid: " + paid + " Dupes Deleted: " + totalDeletionCount;
    }




public Integer findAndDeleteDupes(Integer paid, List<DupeAdAiring> airings, Boolean publish) {
    Map<Integer, List<DupeAdAiring>> airingsByNetwork = airings.stream().collect(Collectors.groupingBy(DupeAdAiring::getChannel));
    Map<Integer, List<DupeAdAiring>> deletionsByNetwork = new HashMap<>();
    AtomicInteger totalDeletionCount = new AtomicInteger(0);

    airingsByNetwork.forEach((networkId, networkAiring) -> {
        List<DupeAdAiring> easternAirings = networkAiring.stream().filter(a -> a.getEasternTs() != null).collect(Collectors.toList());
        List<DupeAdAiring> westernAirings = networkAiring.stream().filter(a -> a.getPacificTs() != null).collect(Collectors.toList());

        List<DupeAdAiring> dupeAiringsToDelete = checkForDupes(networkId, easternAirings, westernAirings);
        LOGGER.debug("Dupe Airings to Delete (Network = {}): {}", networkId, dupeAiringsToDelete.size());

        if (dupeAiringsToDelete.size() > 0) {
            deletionsByNetwork.put(networkId, dupeAiringsToDelete);
            totalDeletionCount.updateAndGet(c -> c + dupeAiringsToDelete.size());
        }
        dupeAiringsToDelete.stream().forEach(a -> {
            deleteAirings(a, publish);
        });
    });

    return totalDeletionCount.intValue();
}



public List<DupeAdAiring> checkForDupes(Integer networkId, List<DupeAdAiring> easternAirings, List<DupeAdAiring> westernAirings) {
    Map<Integer, DupeAdAiring> airingsToDeleteMap = new HashMap<>();
    Map<Integer, List<DupeAdAiring>> easternAiringsByAdId = easternAirings.stream().collect(Collectors.groupingBy(DupeAdAiring::getParentAdId));
    easternAiringsByAdId.forEach((id, eAirings) -> {
        if (eAirings.size() > 1) {
            TreeMap<Integer, DupeAdAiring> airingsTimeMap = new TreeMap<>();
            eAirings.stream().forEach(airing -> {
                if (airing.getEasternTs() == null) {
                    LOGGER.debug("NULL eastern_ts for airing: {}", airing);
                    return;
                }
                // 2024-05-07 - we wanna include overlapping airings that may not have a similar airtime start
                // so we start with a wider radius to get more neighbor airings and then look at those in more detail
                Integer queryStartRadiusSecs = NEIGHBOR_AD_RADIUS_SECS;
                Map<Integer, DupeAdAiring> neighborAiringMap = airingsTimeMap.subMap(airing.getEasternTs() - queryStartRadiusSecs, airing.getEasternTs() + queryStartRadiusSecs);
                if (neighborAiringMap.isEmpty()) {
                    airingsTimeMap.put(airing.getEasternTs(), airing);
                    return;
                }

                List<DupeAdAiring> neighborList = new ArrayList<>();
                neighborList.addAll(neighborAiringMap.values());

                // compare the neighbor airing to the current airing to find dupe candidates
                // by checking for overlap
                List<DupeAdAiring> dupeCandidates = new ArrayList<>();
                // we assume the current airing is the keep airing unless we have evidence otherwise
                DupeAdAiring keepAiring = airing;
                neighborList.stream().forEach(na -> {
                    Integer overlapSeconds = Math.min(keepAiring.getEasternTs(), na.getEasternTs()) - Math.max(keepAiring.getEasternTs(), na.getEasternTs());
                });




                else {
                    // Create list of overlapping airings including the current airing being checked
                    List<DupeAdAiring> overlapAirings = new ArrayList<>();
                    overlapAirings.addAll(overlapAiringsMap.values());
                    overlapAirings.add(airing);

                    // VNA wins
                    // If it doesn't, then 1st one wins
                    // Sort by occurrenceId which guarantees creation time
                    // That way we simply take the first one
                    overlapAirings.sort(Comparator.comparingInt(DupeAdAiring::getOccurrenceId));
                    List<DupeAdAiring> vnaAirings = overlapAirings.stream().filter(oa -> oa.getObservedMarketsCode() != null
                            && oa.getObservedMarketsCode() == 10001).collect(Collectors.toList());
                    DupeAdAiring keepAiring = (vnaAirings == null || vnaAirings.isEmpty()) ? overlapAirings.get(0) : vnaAirings.get(0);

                    // Now remove overlap airing from timestamp and add airingId to the deleteMap
                    overlapAirings.stream().forEach(oa -> {
                        airingsTimeMap.remove(oa.getEasternTs());
                        if (oa.getOccurrenceId() != keepAiring.getOccurrenceId()) {
                            airingsToDeleteMap.put(oa.getOccurrenceId(), oa);
                        }
                    });
                    airingsTimeMap.put(keepAiring.getEasternTs(), keepAiring);
                }
            });
        }
    });

    // Time for west
    Map<Integer, List<DupeAdAiring>> westernAiringsByAdId = westernAirings.stream().collect(Collectors.groupingBy(DupeAdAiring::getParentAdId));
    westernAiringsByAdId.forEach((id, wAirings) -> {
        if (wAirings.size() > 1) {
            //TreeMap<Integer, DupeAdAiring> airingsTimeMap = new TreeMap<>();
            wAirings.stream().forEach(airing -> {
                if (airing.getPacificTs() == null) {
                    LOGGER.debug("NULL pacific_ts for airing: {}", airing);
                    return;
                }
                // Set query radius based on duration of ad
                // Short/Tiny matches require a closer start time to be considered dupes
                Integer queryStartRadiusSecs = (airing.getDuration() == null || airing.getDuration() > SHORT_AD_MAX_DURATION_SECS) ? REGULAR_AD_RADIUS_SECS : SHORT_AD_RADIUS_SECS;
                Map<Integer, DupeAdAiring> overlapAiringsMap = airingsTimeMap.subMap(airing.getPacificTs() - queryStartRadiusSecs, airing.getPacificTs() + queryStartRadiusSecs);
                if (overlapAiringsMap.isEmpty()) {
                    airingsTimeMap.put(airing.getPacificTs(), airing);
                }
                else {
                    // Create list of overlapping airings including the current airing being checked
                    List<DupeAdAiring> overlapAirings = new ArrayList<>();
                    overlapAirings.addAll(overlapAiringsMap.values());
                    overlapAirings.add(airing);

                    // VNA wins
                    // If it doesn't, then 1st one wins
                    // Sort by occurrenceId which guarantees creation time
                    // That way we simply take the first one
                    overlapAirings.sort(Comparator.comparingInt(DupeAdAiring::getOccurrenceId));
                    List<DupeAdAiring> vnaAirings = overlapAirings.stream().filter(oa -> oa.getObservedMarketsCode() != null
                            && oa.getObservedMarketsCode() == 10001).collect(Collectors.toList());
                    DupeAdAiring keepAiring = (vnaAirings == null || vnaAirings.isEmpty()) ? overlapAirings.get(0) : vnaAirings.get(0);

                    // Now remove overlap airing from timestamp and add airingId to the deleteMap
                    overlapAirings.stream().forEach(oa -> {
                        airingsTimeMap.remove(oa.getPacificTs());
                        if (oa.getOccurrenceId() != keepAiring.getOccurrenceId()) {
                            airingsToDeleteMap.put(oa.getOccurrenceId(), oa);
                        }
                    });
                    airingsTimeMap.put(keepAiring.getPacificTs(), keepAiring);
                }
            });
        }
    });

    LOGGER.debug("airingsToDeleteMap.size() = {}", airingsToDeleteMap.size());
    return airingsToDeleteMap.values().stream().collect(Collectors.toList());
}

private void deleteAirings(DupeAdAiring airing, boolean publish) {
    LOGGER.debug("deleteAiring(): publish={}, airing={}", publish, airing);

    if (publish) {
        loccalService.deleteAiring(airing.getOccurrenceId());
    }
    else {
        LOGGER.debug("publish={}, delete from ispot_db.adoccurence where OccurenceID = #{{}}", publish, airing.getOccurrenceId());
    }
}