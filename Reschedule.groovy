// Example API work
// Not all classes and methods are included 

// Perform a basic reschedule with optional replacements
@ResponseBody
@PostMapping("/tools/reschedule/{paid}")
ResponseEntity<?> rescheduleByPaid(@PathVariable String paid,
								   @RequestParam(value = "easternStationId", required = false) Integer easternStationId,
								   @RequestParam(value = "easternMarketId", required = false) Integer easternMarketId,
								   @RequestParam(value = "easternOperatorId", required = false) Integer easternOperatorId,
								   @RequestParam(value = "easternLocalChannelId", required = false) Integer easternLocalChannelId,
								   @RequestParam(value = "eastOffset", defaultValue = "0", required = false) String eastOffset,
								   @RequestParam(value = "westernStationId", required = false) Integer westernStationId,
								   @RequestParam(value = "westernMarketId", required = false) Integer westernMarketId,
								   @RequestParam(value = "westernOperatorId", required = false) Integer westernOperatorId,
								   @RequestParam(value = "westernLocalChannelId", required = false) Integer westernLocalChannelId,
								   @RequestParam(value = "westOffset", defaultValue = "0", required = false) String westOffset) {
	LOGGER.info("Reschedule paid: {}", paid);
	RescheduleHelper rescheduleHelper = new RescheduleHelper();
	rescheduleHelper.buildRescheduleHelper(paid, easternStationId, easternMarketId, easternOperatorId, easternLocalChannelId, eastOffset,
	westernStationId, westernMarketId, westernOperatorId, westernLocalChannelId, westOffset);

	LOGGER.debug("Reschedule Helper: {}", rescheduleHelper);

	String msg = loccalUtil.reschedule(rescheduleHelper);
	if (msg.equals("error")) {
		return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Error completing request " + paid + " did not complete reschedule");
	}
	else {
		return ResponseEntity.status(HttpStatus.OK).body("Reschedule for paid: " + paid + " complete");
	}
}

// Full method for basic reschedule
public String reschedule(RescheduleHelper rescheduleHelper) {
        try {
            MDC.put("airingKey", "reschedule-" + rescheduleHelper.getPaidString());

            List<Map<String, Object>> feeds = loccalService.getFeedsByPaid(Integer.parseInt(rescheduleHelper.getPaidString()));
            if (feeds == null || feeds.isEmpty()) {
                LOGGER.error("No feeds returned for PAID {}", rescheduleHelper.getPaidString());
                return "Error: No feeds returned for PAID";
            }
            feeds.forEach(feed -> {
                LOGGER.debug("{marketid={}, stationid={}, operatorid={}, seattle_channel_id={}, affiliate={}, zone_id={},  start={}, end={}}",
                        feed.get("marketid"), feed.get("stationid"), feed.get("operatorid"), feed.get("seattle_channel_id"),
                        feed.get("affiliate"), feed.get("zone_id"), feed.get("start"), feed.get("end"));
            });

            List<Map<String, Object>> eFeeds = new ArrayList<Map<String, Object>>();
            List<Map<String, Object>> wFeeds = new ArrayList<Map<String, Object>>();
            Map<String, Object> oEasternFeed = null;
            Map<String, Object> oWesternFeed = null;

            if (feeds.size() > 2) {
                LOGGER.error("Unable to reschedule for >2 feed program airing");
                return "Error: Unable to reschedule for >2 feeds";
            }

            if (feeds.size() == 1) {
                LOGGER.warn("Warning: Rescheduleing a single feed program airing");
                eFeeds = feeds.stream().collect(Collectors.toList());

            }

            if (feeds.size() == 2) {
                eFeeds = feeds.stream().filter(feed -> (Integer) feed.get("zone_id") < highestEasternZoneId).collect(Collectors.toList());
                wFeeds = feeds.stream().filter(feed -> (Integer) feed.get("zone_id") >= highestEasternZoneId).collect(Collectors.toList());

                if (eFeeds.size() == 1 && wFeeds.size() == 1) {
                    // east/west pair
                    oEasternFeed = eFeeds.get(0);
                    oWesternFeed = wFeeds.get(0);
                } else if (eFeeds.size() == 2) {
                    // east/east pair
                    if ((Integer) eFeeds.get(0).get("zone_id") < (Integer) eFeeds.get(1).get("zone_id") ||
                            eFeeds.get(0).get("zone_id").equals(eFeeds.get(1).get("zone_id")) && (Integer) eFeeds.get(0).get("marketid") <= (Integer) eFeeds.get(1).get("marketid")) {
                        oEasternFeed = eFeeds.get(0);
                        oWesternFeed = eFeeds.get(1);
                    } else {
                        oEasternFeed = eFeeds.get(1);
                        oWesternFeed = eFeeds.get(0);
                    }
                } else {
                    // west/west pair
                    if ((Integer) wFeeds.get(0).get("zone_id") < (Integer) wFeeds.get(1).get("zone_id") ||
                            wFeeds.get(0).get("zone_id").equals(wFeeds.get(1).get("zone_id")) && (Integer) wFeeds.get(0).get("marketid") <= (Integer) wFeeds.get(1).get("marketid")) {
                        oEasternFeed = wFeeds.get(0);
                        oWesternFeed = wFeeds.get(1);
                    } else {
                        oEasternFeed = wFeeds.get(1);
                        oWesternFeed = wFeeds.get(0);
                    }
                }
            }

            Map<String, Object> singleFeed = null;
            if (feeds.size() == 1) {
                singleFeed = eFeeds.get(0);
                LOGGER.info("Single feed detected: {}", singleFeed);

                if (singleFeed == null) {
                    LOGGER.error("Unable to determine single feed for PAID {}", rescheduleHelper.getPaidString());
                    return "Error: Unable to determine single feed for PAID: " + rescheduleHelper.getPaidString();
                }
            } else {
                if (oEasternFeed == null) {
                    LOGGER.error("Unable to determine eastern feed for PAID {}", rescheduleHelper.getPaidString());
                    return "Error: Unable to determine eastern feed for PAID: " + rescheduleHelper.getPaidString();
                }
                if (oEasternFeed.get("stationid") == null || oEasternFeed.get("marketid") == null || oEasternFeed.get("operatorid") == null) {
                    LOGGER.error("Missing INFO for eastern feed: {}", oEasternFeed);
                    return "Error: Missing INFO for eastern feed: " + oEasternFeed;
                }

                if (oWesternFeed == null) {
                    LOGGER.error("Unable to determine western feed for PAID {}", rescheduleHelper.getPaidString());
                    return "Error: Unable to determine western feed for PAID: " + rescheduleHelper.getPaidString();
                }
                if (oWesternFeed.get("stationid") == null || oWesternFeed.get("marketid") == null || oWesternFeed.get("operatorid") == null) {
                    LOGGER.error("Missing INFO for western feed: {}", oWesternFeed);
                    return "Error: Missing INFO for western feed: " + oWesternFeed;
                }
            }

            // Check for locked program airings
            List<Integer> paids = new ArrayList<>();
            paids.add(Integer.parseInt(rescheduleHelper.getPaidString()));
            List<Integer> lockStatusList = loccalService.getLockStatusByPaid(paids);
            LOGGER.debug("List of locked PAIDs: {}", lockStatusList);

            if (!lockStatusList.isEmpty()) {
                return "Error: PAID " + lockStatusList + " is locked and cannot be resent";
            }

            // Delete airings
            LOGGER.info("Deleting occurrences for programAiringId {}", rescheduleHelper.getPaidString());
            loccalService.deleteOccurrenceForPaid(Integer.parseInt(rescheduleHelper.getPaidString()));

            //Flush
            String flushCmd = adcalHostname + "/adcal/rest/cache/flushProgramAiringId/" + rescheduleHelper.getPaidString();
            LOGGER.debug("Flush command: {}", flushCmd);
            RestTemplate restTemplate = new RestTemplate();
            String result = restTemplate.postForObject(flushCmd, null, String.class);
            LOGGER.info("{}", result);

            // Reschedule
            if (oEasternFeed != null && oWesternFeed != null) {
                // Multiple feeds
                // Validate eastern feed parameters
                if (oEasternFeed.get("stationid") == null || oEasternFeed.get("marketid") == null || oEasternFeed.get("operatorid") == null || oEasternFeed.get("start") == null || oEasternFeed.get("end") == null) {
                    LOGGER.error("Null/Missing fields for eastern feed");
                    return "Error: Null/Missing fields for eastern feed";
                }
                if (oWesternFeed.get("stationid") == null || oWesternFeed.get("marketid") == null || oWesternFeed.get("operatorid") == null || oWesternFeed.get("start") == null || oWesternFeed.get("end") == null) {
                    LOGGER.error("Null/Missing fields for western feed");
                    return "Error: Null/Missing fields for western feed";
                }


                List<RawAdAiring> easternRawAdAirings = new ArrayList<RawAdAiring>();
                List<RawAdAiring> westernRawAdAirings = new ArrayList<RawAdAiring>();
                LocalDateTime easternStart = null;
                LocalDateTime easternEnd = null;
                LocalDateTime westernStart = null;
                LocalDateTime westernEnd = null;

                LOGGER.info("Getting raw ad airings for Eastern Feed");
                if (rescheduleHelper.getReplaceStationOne() == -1) {

                    Timestamp easternStartTs = (Timestamp) oEasternFeed.get("start");
                    easternStart = easternStartTs.toLocalDateTime();
                    Timestamp easternEndTs = (Timestamp) oEasternFeed.get("end");
                    easternEnd = easternEndTs.toLocalDateTime();

                    LOGGER.debug("station: {}, market: {}, oper: {}, start: {}, end: {}", oEasternFeed.get("stationid"), oEasternFeed.get("marketid"), oEasternFeed.get("operatorid"), easternStart, easternEnd);
                    easternRawAdAirings = loccalService.gatherRawAdAirings((Integer) oEasternFeed.get("stationid"),
                            (Integer) oEasternFeed.get("marketid"), (Integer) oEasternFeed.get("operatorid"), easternStart, easternEnd, rescheduleHelper.getReplaceStationOne(),
                            rescheduleHelper.getReplaceMarketOne(), rescheduleHelper.getReplaceOperatorOne(), rescheduleHelper.getReplaceChannelOne(), Integer.parseInt(rescheduleHelper.getDestOffset()));
                }
                else {

                    Timestamp easternStartTs = (Timestamp) oEasternFeed.get("start");
                    easternStart = easternStartTs.toLocalDateTime().plusMinutes(Integer.parseInt(rescheduleHelper.getDestOffset()));
                    Timestamp easternEndTs = (Timestamp) oEasternFeed.get("end");
                    easternEnd = easternEndTs.toLocalDateTime().plusMinutes(Integer.parseInt(rescheduleHelper.getDestOffset()));
                    rescheduleHelper.setDestOffset(String.valueOf(-Integer.parseInt(rescheduleHelper.getDestOffset())));

                    LOGGER.debug("station: {}, market: {}, oper: {}, start: {}, end: {}", oEasternFeed.get("stationid"), oEasternFeed.get("marketid"), oEasternFeed.get("operatorid"), easternStart, easternEnd);
                    easternRawAdAirings = loccalService.gatherRawAdAirings(rescheduleHelper.getReplaceStationOne(),
                            rescheduleHelper.getReplaceMarketOne(), rescheduleHelper.getReplaceOperatorOne(), easternStart, easternEnd, (Integer) oEasternFeed.get("stationid"),
                            (Integer) oEasternFeed.get("marketid"), (Integer) oEasternFeed.get("operatorid"), rescheduleHelper.getReplaceChannelOne(), Integer.parseInt(rescheduleHelper.getDestOffset()));
                }

                LOGGER.info("Getting raw ad airings for Western Feed");
                if (rescheduleHelper.getReplaceStationTwo() == -1) {

                    Timestamp westernStartTs = (Timestamp) oWesternFeed.get("start");
                    westernStart = westernStartTs.toLocalDateTime();
                    Timestamp westernEndTs = (Timestamp) oWesternFeed.get("end");
                    westernEnd = westernEndTs.toLocalDateTime();

                    LOGGER.debug("station: {}, market: {}, oper: {}, start: {}, end: {}", oWesternFeed.get("stationid"), oWesternFeed.get("marketid"), oWesternFeed.get("operatorid"), westernStart, westernEnd);
                    westernRawAdAirings = loccalService.gatherRawAdAirings((Integer) oWesternFeed.get("stationid"),
                            (Integer) oWesternFeed.get("marketid"), (Integer) oWesternFeed.get("operatorid"), westernStart, westernEnd, rescheduleHelper.getReplaceStationTwo(),
                            rescheduleHelper.getReplaceMarketTwo(), rescheduleHelper.getReplaceOperatorTwo(), rescheduleHelper.getReplaceChannelTwo(), Integer.parseInt(rescheduleHelper.getSourceOffset()));
                }
                else {

                    Timestamp westernStartTs = (Timestamp) oWesternFeed.get("start");
                    westernStart = westernStartTs.toLocalDateTime().plusMinutes(Integer.parseInt(rescheduleHelper.getSourceOffset()));
                    Timestamp westernEndTs = (Timestamp) oWesternFeed.get("end");
                    westernEnd = westernEndTs.toLocalDateTime().plusMinutes(Integer.parseInt(rescheduleHelper.getSourceOffset()));
                    rescheduleHelper.setSourceOffset(String.valueOf(-Integer.parseInt(rescheduleHelper.getSourceOffset())));

                    LOGGER.debug("station: {}, market: {}, oper: {}, start: {}, end: {}", oWesternFeed.get("stationid"), oWesternFeed.get("marketid"), oWesternFeed.get("operatorid"), westernStart, westernEnd);
                    westernRawAdAirings = loccalService.gatherRawAdAirings(rescheduleHelper.getReplaceStationTwo(),
                            rescheduleHelper.getReplaceMarketTwo(), rescheduleHelper.getReplaceOperatorTwo(), westernStart, westernEnd, (Integer) oWesternFeed.get("stationid"),
                            (Integer) oWesternFeed.get("marketid"), (Integer) oWesternFeed.get("operatorid"), rescheduleHelper.getReplaceChannelTwo(), Integer.parseInt(rescheduleHelper.getSourceOffset()));
                }

                try {

                    // Flush market one from schedule cache
                    this.flushScheduleByStation(String.valueOf(oEasternFeed.get("stationid")));

                    LOGGER.info("Will re-send eastern airings for station {} in market {}", oEasternFeed.get("stationid"), oEasternFeed.get("marketid"));
                    loccalService.forwardRescheduleToReportRawAdAiring(easternRawAdAirings);

                    LOGGER.info("Sleeping 10 seconds to allow east to be processed before west");
                    Thread.sleep(10000);

                    // Flush market two from schedule cache
                    this.flushScheduleByStation(String.valueOf(oWesternFeed.get("stationid")));

                    LOGGER.info("Will re-send western airings for station {} in market {}", oWesternFeed.get("stationid"), oWesternFeed.get("marketid"));
                    loccalService.forwardRescheduleToReportRawAdAiring(westernRawAdAirings);

                } catch (java.lang.InterruptedException e) {
                    LOGGER.error("Error sending raw airings", e);
                }
            }
            else {
                // Single feed reschedule
                // Validation
                if (singleFeed.get("stationid") == null || singleFeed.get("marketid") == null || singleFeed.get("operatorid") == null || singleFeed.get("start") == null || singleFeed.get("end") == null) {
                    LOGGER.error("Null/Missing fields for single feed");
                    return "Error: Null/Missing fields for single feed";
                }

                Integer replacementSingleStation = rescheduleHelper.getReplaceStationOne() != -1 ? rescheduleHelper.getReplaceStationOne() : rescheduleHelper.getReplaceStationTwo();
                Integer replacementSingleMarket = rescheduleHelper.getReplaceMarketOne() != -1 ? rescheduleHelper.getReplaceMarketOne() : rescheduleHelper.getReplaceMarketTwo();
                Integer replacementSingleOperator = rescheduleHelper.getReplaceOperatorOne() != -1 ? rescheduleHelper.getReplaceOperatorOne() : rescheduleHelper.getReplaceOperatorTwo();
                Integer replacementSingleChannel = rescheduleHelper.getReplaceChannelOne() != -1 ? rescheduleHelper.getReplaceChannelOne() : rescheduleHelper.getReplaceChannelTwo();
                Integer singleOffset = Integer.valueOf(!rescheduleHelper.getDestOffset().equals("0") ? rescheduleHelper.getDestOffset() : rescheduleHelper.getSourceOffset());

                List<RawAdAiring> rawAdAiringList;

                if (replacementSingleStation == singleStationReplacement) {

                    Timestamp startTs = (Timestamp) singleFeed.get("start");
                    LocalDateTime start = startTs.toLocalDateTime();
                    Timestamp endTs = (Timestamp) singleFeed.get("end");
                    LocalDateTime end = endTs.toLocalDateTime();

                    LOGGER.debug("station: {}, market: {}, oper: {}, start: {}, end: {}", singleFeed.get("stationid"), singleFeed.get("marketid"), singleFeed.get("operatorid"), start, end);
                    rawAdAiringList = loccalService.gatherRawAdAirings((Integer) singleFeed.get("stationid"),
                            (Integer) singleFeed.get("marketid"), (Integer) singleFeed.get("operatorid"), start, end, replacementSingleStation, replacementSingleMarket,
                            replacementSingleOperator, replacementSingleChannel, singleOffset);
                }
                else {

                    Timestamp startTs = (Timestamp) singleFeed.get("start");
                    LocalDateTime start = startTs.toLocalDateTime().plusMinutes(singleOffset);
                    Timestamp endTs = (Timestamp) singleFeed.get("end");
                    LocalDateTime end = endTs.toLocalDateTime().plusMinutes(singleOffset);
                    singleOffset = -singleOffset;

                    LOGGER.debug("station: {}, market: {}, oper: {}, start: {}, end: {}", singleFeed.get("stationid"), singleFeed.get("marketid"), singleFeed.get("operatorid"), start, end);
                    rawAdAiringList = loccalService.gatherRawAdAirings(replacementSingleStation,
                            replacementSingleMarket, replacementSingleOperator, start, end, (Integer) singleFeed.get("stationid"),
                            (Integer) singleFeed.get("marketid"), (Integer) singleFeed.get("operatorid"), replacementSingleChannel, singleOffset);
                }

                this.flushScheduleByStation(String.valueOf(singleFeed.get("stationid")));

                LOGGER.info("Will re-send airings for station {} in market {}", singleFeed.get("stationid"), singleFeed.get("marketid"));
                loccalService.forwardRescheduleToReportRawAdAiring(rawAdAiringList);
            }
        } catch (Exception ex) {
            LOGGER.error("Exception caught rescheduleing program airing " + rescheduleHelper.getPaidString(), ex);
        } finally {
            MDC.clear();
        }
        return "Reschedule complete";
    }