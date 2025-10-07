/*
 * Copyright 2015-2025 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.valkey.springframework.data.valkey.connection;

import org.springframework.data.geo.Point;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author Dennis Neufeld
 */
public interface ClusterConnectionTests {

	Point POINT_ARIGENTO = new Point(13.583333, 37.316667);
	Point POINT_CATANIA = new Point(15.087269, 37.502669);
	Point POINT_PALERMO = new Point(13.361389, 38.115556);

	// DATAVALKEY-315
	void appendShouldAddValueCorrectly();

	// DATAVALKEY-315
	void bRPopLPushShouldWork();

	// DATAVALKEY-315
	void bRPopLPushShouldWorkOnSameSlotKeys();

	// DATAVALKEY-315
	void bitCountShouldWorkCorrectly();

	// DATAVALKEY-315
	void bitCountWithRangeShouldWorkCorrectly();

	// DATAVALKEY-315
	void bitOpShouldThrowExceptionWhenKeysDoNotMapToSameSlot();

	// DATAVALKEY-315
	void blPopShouldPopElementCorrectly();

	// DATAVALKEY-315
	void blPopShouldPopElementCorrectlyWhenKeyOnSameSlot();

	// DATAVALKEY-315
	void brPopShouldPopElementCorrectly();

	// DATAVALKEY-315
	void brPopShouldPopElementCorrectlyWhenKeyOnSameSlot();

	// DATAVALKEY-315
	void clientListShouldGetInfosForAllClients();

	// DATAVALKEY-315
	void clusterGetMasterReplicaMapShouldListMastersAndReplicasCorrectly();

	// DATAVALKEY-315
	void clusterGetReplicasShouldReturnReplicaCorrectly();

	// DATAVALKEY-315
	void countKeysShouldReturnNumberOfKeysInSlot();

	// DATAVALKEY-315
	void dbSizeForSpecificNodeShouldGetNodeDbSize();

	// DATAVALKEY-315
	void dbSizeShouldReturnCummulatedDbSize();

	// DATAVALKEY-315
	void decrByShouldDecreaseValueCorrectly();

	// DATAVALKEY-315
	void decrShouldDecreaseValueCorrectly();

	// DATAVALKEY-315
	void delShouldRemoveMultipleKeysCorrectly();

	// DATAVALKEY-315
	void delShouldRemoveMultipleKeysOnSameSlotCorrectly();

	// DATAVALKEY-315
	void delShouldRemoveSingleKeyCorrectly();

	// DATAVALKEY-315
	void discardShouldThrowException();

	// DATAVALKEY-315
	void dumpAndRestoreShouldWorkCorrectly();

	// DATAVALKEY-696
	void dumpAndRestoreWithReplaceOptionShouldWorkCorrectly();

	// DATAVALKEY-315
	void echoShouldReturnInputCorrectly();

	// DATAVALKEY-315
	void execShouldThrowException();

	// DATAVALKEY-529
	void existsShouldCountSameKeyMultipleTimes();

	// DATAVALKEY-529
	void existsWithMultipleKeysShouldConsiderAbsentKeys();

	// DATAVALKEY-529
	void existsWithMultipleKeysShouldReturnResultCorrectly();

	// DATAVALKEY-315
	void expireAtShouldBeSetCorrectly();

	// DATAVALKEY-315
	void expireShouldBeSetCorrectly();

	// DATAVALKEY-315
	void flushDbOnSingleNodeShouldFlushOnlyGivenNodesDb();

	// GH-2187
	void flushDbSyncOnSingleNodeShouldFlushOnlyGivenNodesDb();

	// GH-2187
	void flushDbAsyncOnSingleNodeShouldFlushOnlyGivenNodesDb();

	// DATAVALKEY-315
	void flushDbShouldFlushAllClusterNodes();

	// GH-2187
	void flushDbSyncShouldFlushAllClusterNodes();

	// GH-2187
	void flushDbAsyncShouldFlushAllClusterNodes();

	// GH-2187
	void flushAllOnSingleNodeShouldFlushOnlyGivenNodesDb();

	// GH-2187
	void flushAllSyncOnSingleNodeShouldFlushOnlyGivenNodesDb();

	// GH-2187
	void flushAllAsyncOnSingleNodeShouldFlushOnlyGivenNodesDb();

	// GH-2187
	void flushAllShouldFlushAllClusterNodes();

	// GH-2187
	void flushAllSyncShouldFlushAllClusterNodes();

	// GH-2187
	void flushAllAsyncShouldFlushAllClusterNodes();

	// DATAVALKEY-438
	void geoAddMultipleGeoLocations();

	// DATAVALKEY-438
	void geoAddSingleGeoLocation();

	// DATAVALKEY-438
	void geoDist();

	// DATAVALKEY-438
	void geoDistWithMetric();

	// DATAVALKEY-438
	void geoHash();

	// DATAVALKEY-438
	void geoHashNonExisting();

	// DATAVALKEY-438
	void geoPosition();

	// DATAVALKEY-438
	void geoPositionNonExisting();

	// DATAVALKEY-438
	void geoRadiusByMemberShouldApplyLimit();

	// DATAVALKEY-438
	void geoRadiusByMemberShouldReturnDistanceCorrectly();

	// DATAVALKEY-438
	void geoRadiusByMemberShouldReturnMembersCorrectly();

	// DATAVALKEY-438
	void geoRadiusShouldApplyLimit();

	// DATAVALKEY-438
	void geoRadiusShouldReturnDistanceCorrectly();

	// DATAVALKEY-438
	void geoRadiusShouldReturnMembersCorrectly();

	// DATAVALKEY-438
	void geoRemoveDeletesMembers();

	// DATAVALKEY-315
	void getBitShouldWorkCorrectly();

	// DATAVALKEY-315
	void getClusterNodeForKeyShouldReturnNodeCorrectly();

	// DATAVALKEY-315
	void getConfigShouldLoadConfigurationOfSpecificNode();

	// DATAVALKEY-315
	void getConfigShouldLoadCumulatedConfiguration();

	// DATAVALKEY-315
	void getRangeShouldReturnValueCorrectly();

	// GH-2050
	void getExShouldWorkCorrectly();

	// GH-2050
	void getDelShouldWorkCorrectly();

	// DATAVALKEY-315
	void getSetShouldWorkCorrectly();

	// DATAVALKEY-315
	void getShouldReturnValueCorrectly();

	// DATAVALKEY-315
	void hDelShouldRemoveFieldsCorrectly();

	// DATAVALKEY-315
	void hExistsShouldReturnPresenceOfFieldCorrectly();

	// DATAVALKEY-315
	void hGetAllShouldRetrieveEntriesCorrectly();

	// DATAVALKEY-315
	void hGetShouldRetrieveValueCorrectly();

	// DATAVALKEY-315
	void hIncrByFloatShouldIncreaseFieldCorretly();

	// DATAVALKEY-315
	void hIncrByShouldIncreaseFieldCorretly();

	// DATAVALKEY-315
	void hKeysShouldRetrieveKeysCorrectly();

	// DATAVALKEY-315
	void hLenShouldRetrieveSizeCorrectly();

	// DATAVALKEY-315
	void hMGetShouldRetrieveValueCorrectly();

	// DATAVALKEY-315
	void hMSetShouldAddValuesCorrectly();

	// DATAVALKEY-479
	public void hScanShouldReadEntireValueRange();

	// DATAVALKEY-315
	void hSetNXShouldNotSetValueWhenAlreadyExists();

	// DATAVALKEY-315
	void hSetNXShouldSetValueCorrectly();

	// DATAVALKEY-315
	void hSetShouldSetValueCorrectly();

	// DATAVALKEY-698
	void hStrLenReturnsFieldLength();

	// DATAVALKEY-698
	void hStrLenReturnsZeroWhenFieldDoesNotExist();

	// DATAVALKEY-698
	void hStrLenReturnsZeroWhenKeyDoesNotExist();

	// DATAVALKEY-315
	void hValsShouldRetrieveValuesCorrectly();

	// DATAVALKEY-315
	void incrByFloatShouldIncreaseValueCorrectly();

	// DATAVALKEY-315
	void incrByShouldIncreaseValueCorrectly();

	// DATAVALKEY-315
	void incrShouldIncreaseValueCorrectly();

	// DATAVALKEY-315
	void infoShouldCollectInfoForSpecificNode();

	// DATAVALKEY-315
	void infoShouldCollectInfoForSpecificNodeAndSection();

	// DATAVALKEY-315
	void infoShouldCollectionInfoFromAllClusterNodes();

	// DATAVALKEY-315
	void keysShouldReturnAllKeys();

	// DATAVALKEY-315
	void keysShouldReturnAllKeysForSpecificNode();

	// DATAVALKEY-635
	void scanShouldReturnAllKeys();

	// DATAVALKEY-635
	void scanShouldReturnAllKeysForSpecificNode();

	// DATAVALKEY-315
	void lIndexShouldGetElementAtIndexCorrectly();

	// DATAVALKEY-315
	void lInsertShouldAddElementAtPositionCorrectly();

	// GH-2039
	void lMoveShouldMoveElementsCorrectly();

	// GH-2039
	void blMoveShouldMoveElementsCorrectly();

	// DATAVALKEY-315
	void lLenShouldCountValuesCorrectly();

	// DATAVALKEY-315
	void lPopShouldReturnElementCorrectly();

	// DATAVALKEY-315
	void lPushNXShouldNotAddValuesWhenKeyDoesNotExist();

	// DATAVALKEY-315
	void lPushShouldAddValuesCorrectly();

	// DATAVALKEY-315
	void lRangeShouldGetValuesCorrectly();

	// DATAVALKEY-315
	void lRemShouldRemoveElementAtPositionCorrectly();

	// DATAVALKEY-315
	void lSetShouldSetElementAtPositionCorrectly();

	// DATAVALKEY-315
	void lTrimShouldTrimListCorrectly();

	// DATAVALKEY-315
	void mGetShouldReturnCorrectlyWhenKeysDoNotMapToSameSlot();

	// DATAVALKEY-756
	void mGetShouldReturnMultipleSameKeysWhenKeysDoNotMapToSameSlot();

	// DATAVALKEY-315
	void mGetShouldReturnCorrectlyWhenKeysMapToSameSlot();

	// DATAVALKEY-756
	void mGetShouldReturnMultipleSameKeysWhenKeysMapToSameSlot();

	// DATAVALKEY-315
	void mSetNXShouldReturnFalseIfNotAllKeysSet();

	// DATAVALKEY-315
	void mSetNXShouldReturnTrueIfAllKeysSet();

	// DATAVALKEY-315
	void mSetNXShouldWorkForOnSameSlotKeys();

	// DATAVALKEY-315
	void mSetShouldWorkWhenKeysDoNotMapToSameSlot();

	// DATAVALKEY-315
	void mSetShouldWorkWhenKeysMapToSameSlot();

	// DATAVALKEY-315
	void moveShouldNotBeSupported();

	// DATAVALKEY-315
	void multiShouldThrowException();

	// DATAVALKEY-315
	void pExpireAtShouldBeSetCorrectly();

	// DATAVALKEY-315
	void pExpireShouldBeSetCorrectly();

	// DATAVALKEY-315
	void pSetExShouldSetValueCorrectly();

	// DATAVALKEY-315
	void pTtlShouldReturValueCorrectly();

	// DATAVALKEY-315
	void pTtlShouldReturnMinusOneWhenKeyDoesNotHaveExpirationSet();

	// DATAVALKEY-315
	void pTtlShouldReturnMinusTwoWhenKeyDoesNotExist();

	// DATAVALKEY-526
	void pTtlWithTimeUnitShouldReturnMinusTwoWhenKeyDoesNotExist();

	// DATAVALKEY-315
	void persistShouldRemoveTTL();

	void pfAddShouldAddValuesCorrectly();

	// DATAVALKEY-315
	void pfCountShouldAllowCountingOnSameSlotKeys();

	// DATAVALKEY-315
	void pfCountShouldAllowCountingOnSingleKey();

	// DATAVALKEY-315
	void pfCountShouldThrowErrorCountingOnDifferentSlotKeys();

	// DATAVALKEY-315
	public void pfMergeShouldThrowErrorOnDifferentSlotKeys();

	// DATAVALKEY-315
	void pfMergeShouldWorkWhenAllKeysMapToSameSlot();

	// DATAVALKEY-315
	void pingShouldRetrunPong();

	// DATAVALKEY-315
	void pingShouldRetrunPongForExistingNode();

	// DATAVALKEY-315
	void pingShouldThrowExceptionWhenNodeNotKnownToCluster();

	// DATAVALKEY-315
	void rPopLPushShouldWorkWhenDoNotMapToSameSlot();

	// DATAVALKEY-315
	public void rPopLPushShouldWorkWhenKeysOnSameSlot();

	// DATAVALKEY-315
	void rPopShouldReturnElementCorrectly();

	// DATAVALKEY-315
	void rPushNXShouldNotAddValuesWhenKeyDoesNotExist();

	// DATAVALKEY-315
	void rPushShouldAddValuesCorrectly();

	// DATAVALKEY-315
	void randomKeyShouldReturnCorrectlyWhenKeysAvailable();

	// DATAVALKEY-315
	void randomKeyShouldReturnNullWhenNoKeysAvailable();

	// DATAVALKEY-315
	void rename();

	// DATAVALKEY-1190
	void renameShouldOverwriteTargetKey();

	// DATAVALKEY-315
	void renameNXWhenOnSameSlot();

	// DATAVALKEY-315
	void renameNXWhenTargetKeyDoesExist();

	// DATAVALKEY-315
	void renameNXWhenTargetKeyDoesNotExist();

	// DATAVALKEY-315
	void renameSameKeysOnSameSlot();

	// DATAVALKEY-315
	void sAddShouldAddValueToSetCorrectly();

	// DATAVALKEY-315
	void sCardShouldCountValuesInSetCorrectly();

	// DATAVALKEY-315
	void sDiffShouldWorkWhenKeysMapToSameSlot();

	// DATAVALKEY-315
	void sDiffShouldWorkWhenKeysNotMapToSameSlot();

	// DATAVALKEY-315
	void sDiffStoreShouldWorkWhenKeysMapToSameSlot();

	// DATAVALKEY-315
	void sDiffStoreShouldWorkWhenKeysNotMapToSameSlot();

	// DATAVALKEY-315
	void sInterShouldWorkForKeysMappingToSameSlot();

	// DATAVALKEY-315
	void sInterShouldWorkForKeysNotMappingToSameSlot();

	// DATAVALKEY-315
	void sInterStoreShouldWorkForKeysMappingToSameSlot();

	// DATAVALKEY-315
	void sInterStoreShouldWorkForKeysNotMappingToSameSlot();

	// DATAVALKEY-315
	void sIsMemberShouldReturnFalseIfValueIsMemberOfSet();

	// DATAVALKEY-315
	void sIsMemberShouldReturnTrueIfValueIsMemberOfSet();

	// GH-2037
	void sMIsMemberShouldReturnCorrectValues();

	// DATAVALKEY-315
	void sMembersShouldReturnValuesContainedInSetCorrectly();

	// DATAVALKEY-315
	void sMoveShouldWorkWhenKeysDoNotMapToSameSlot();

	// DATAVALKEY-315
	void sMoveShouldWorkWhenKeysMapToSameSlot();

	// DATAVALKEY-315
	void sPopShouldPopValueFromSetCorrectly();

	// DATAVALKEY-315
	void sRandMamberShouldReturnValueCorrectly();

	// DATAVALKEY-315
	void sRandMamberWithCountShouldReturnValueCorrectly();

	// DATAVALKEY-315
	void sRemShouldRemoveValueFromSetCorrectly();

	// DATAVALKEY-315
	void sUnionShouldWorkForKeysMappingToSameSlot();

	// DATAVALKEY-315
	void sUnionShouldWorkForKeysNotMappingToSameSlot();

	// DATAVALKEY-315
	void sUnionStoreShouldWorkForKeysMappingToSameSlot();

	// DATAVALKEY-315
	void sUnionStoreShouldWorkForKeysNotMappingToSameSlot();

	// DATAVALKEY-315
	void selectShouldAllowSelectionOfDBIndexZero();

	// DATAVALKEY-315
	void selectShouldThrowExceptionWhenSelectingNonZeroDbIndex();

	// DATAVALKEY-315
	void setBitShouldWorkCorrectly();

	// DATAVALKEY-315
	void setExShouldSetValueCorrectly();

	// DATAVALKEY-315
	void setNxShouldNotSetValueWhenAlreadyExistsInDBCorrectly();

	// DATAVALKEY-315
	void setNxShouldSetValueCorrectly();

	// DATAVALKEY-315
	void setRangeShouldWorkCorrectly();

	// DATAVALKEY-315
	void setShouldSetValueCorrectly();

	// DATAVALKEY-316
	void setWithExpirationAndIfAbsentShouldNotBeAppliedWhenKeyExists();

	// DATAVALKEY-316
	void setWithExpirationAndIfAbsentShouldWorkCorrectly();

	// DATAVALKEY-316
	void setWithExpirationAndIfPresentShouldNotBeAppliedWhenKeyDoesNotExists();

	// DATAVALKEY-316
	void setWithExpirationAndIfPresentShouldWorkCorrectly();

	// DATAVALKEY-316
	void setWithExpirationInMillisecondsShouldWorkCorrectly();

	// DATAVALKEY-316
	void setWithExpirationInSecondsShouldWorkCorrectly();

	// DATAVALKEY-316
	void setWithOptionIfAbsentShouldWorkCorrectly();

	// DATAVALKEY-316
	void setWithOptionIfPresentShouldWorkCorrectly();

	// DATAVALKEY-315

	// DATAVALKEY-315
	void shouldAllowSettingAndGettingValues();

	// DATAVALKEY-315
	void sortAndStoreShouldAddSortedValuesValuesCorrectly();

	// DATAVALKEY-315, GH-2341
	void sortAndStoreShouldReplaceDestinationList();

	// DATAVALKEY-315
	void sortShouldReturnValuesCorrectly();

	// DATAVALKEY-315
	void sscanShouldRetrieveAllValuesInSetCorrectly();

	// DATAVALKEY-315
	void strLenShouldWorkCorrectly();

	// DATAVALKEY-315
	void ttlShouldReturnMinusOneWhenKeyDoesNotHaveExpirationSet();

	// DATAVALKEY-315
	void ttlShouldReturnMinusTwoWhenKeyDoesNotExist();

	// DATAVALKEY-315
	void ttlShouldReturnValueCorrectly();

	// DATAVALKEY-526
	void ttlWithTimeUnitShouldReturnMinusTwoWhenKeyDoesNotExist();

	// DATAVALKEY-315
	void typeShouldReadKeyTypeCorrectly();

	// DATAVALKEY-315
	void unwatchShouldThrowException();

	// DATAVALKEY-315
	void watchShouldThrowException();

	// DATAVALKEY-315
	void zAddShouldAddValueWithScoreCorrectly();

	// DATAVALKEY-315
	void zCardShouldReturnTotalNumberOfValues();

	// DATAVALKEY-315
	void zCountShouldCountValuesInRange();

	// DATAVALKEY-315
	void zIncrByShouldIncScoreForValueCorrectly();

	// GH-2041
	void zDiffShouldThrowExceptionWhenKeysDoNotMapToSameSlots();

	// GH-2041
	void zDiffShouldWorkForSameSlotKeys();

	// GH-2041
	void zDiffStoreShouldWorkForSameSlotKeys();

	// GH-2042
	void zInterShouldThrowExceptionWhenKeysDoNotMapToSameSlots();

	// GH-2042
	void zInterShouldWorkForSameSlotKeys();

	// DATAVALKEY-315
	void zInterStoreShouldThrowExceptionWhenKeysDoNotMapToSameSlots();

	// DATAVALKEY-315
	void zInterStoreShouldWorkForSameSlotKeys();

	// GH-2007
	void zPopMinShouldWorkCorrectly();

	// GH-2007
	void bzPopMinShouldWorkCorrectly();

	// GH-2007
	void zPopMaxShouldWorkCorrectly();

	// GH-2007
	void bzPopMaxShouldWorkCorrectly();

	// GH-2049
	void zRandMemberShouldReturnResultCorrectly();

	// GH-2049
	void zRandMemberWithScoreShouldReturnResultCorrectly();

	// DATAVALKEY-315
	void zRangeByLexShouldReturnResultCorrectly();

	// DATAVALKEY-315
	void zRangeByScoreShouldReturnValuesCorrectly();

	// DATAVALKEY-315
	void zRangeByScoreShouldReturnValuesCorrectlyWhenGivenOffsetAndScore();

	// DATAVALKEY-315
	void zRangeByScoreWithScoresShouldReturnValuesAndScoreCorrectly();

	// DATAVALKEY-315
	void zRangeByScoreWithScoresShouldReturnValuesCorrectlyWhenGivenOffsetAndScore();

	// DATAVALKEY-315
	void zRangeShouldReturnValuesCorrectly();

	// DATAVALKEY-315
	void zRangeWithScoresShouldReturnValuesAndScoreCorrectly();

	// DATAVALKEY-315
	void zRankShouldReturnPositionForValueCorrectly();

	// DATAVALKEY-315
	void zRankShouldReturnReversePositionForValueCorrectly();

	// DATAVALKEY-315
	void zRemRangeByScoreShouldRemoveValues();

	// DATAVALKEY-315
	void zRemRangeShouldRemoveValues();

	// DATAVALKEY-315
	void zRemShouldRemoveValueWithScoreCorrectly();

	// DATAVALKEY-315
	void zRevRangeByScoreShouldReturnValuesCorrectly();

	// DATAVALKEY-315
	void zRevRangeByScoreShouldReturnValuesCorrectlyWhenGivenOffsetAndScore();

	// DATAVALKEY-315
	void zRevRangeByScoreWithScoresShouldReturnValuesAndScoreCorrectly();

	// DATAVALKEY-315
	void zRevRangeByScoreWithScoresShouldReturnValuesCorrectlyWhenGivenOffsetAndScore();

	// DATAVALKEY-315
	void zRevRangeShouldReturnValuesCorrectly();

	// DATAVALKEY-315
	void zRevRangeWithScoresShouldReturnValuesAndScoreCorrectly();

	// DATAVALKEY-479
	void zScanShouldReadEntireValueRange();

	// DATAVALKEY-315
	void zScoreShouldRetrieveScoreForValue();

	// GH-2038
	void zMScoreShouldRetrieveScoreForValues();

	// GH-2042
	void zUnionShouldThrowExceptionWhenKeysDoNotMapToSameSlots();

	// GH-2042
	void zUnionShouldWorkForSameSlotKeys();

	// DATAVALKEY-315
	void zUnionStoreShouldThrowExceptionWhenKeysDoNotMapToSameSlots();

	// DATAVALKEY-315
	void zUnionStoreShouldWorkForSameSlotKeys();

}
