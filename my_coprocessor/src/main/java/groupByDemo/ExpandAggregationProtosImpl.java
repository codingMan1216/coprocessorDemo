package groupByDemo;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.protobuf.ByteString;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;

import groupByDemo.protoFiles.ExpandAggregationProtos.ExpandAggregationRequest;
import groupByDemo.protoFiles.ExpandAggregationProtos.ExpandAggregationResponse;
import groupByDemo.protoFiles.ExpandAggregationProtos.ExpandAggregationService;
import groupByDemo.protoFiles.ExpandAggregationProtos.ExpandCell;
import groupByDemo.protoFiles.ExpandAggregationProtos.ExpandRow;

public class ExpandAggregationProtosImpl extends ExpandAggregationService implements CoprocessorService, Coprocessor {
	private RegionCoprocessorEnvironment env;

	@Override
	public void getGroupSumAndCount(RpcController controller, ExpandAggregationRequest request,
			RpcCallback<ExpandAggregationResponse> done) {
		InternalScanner scanner = null;
		ExpandAggregationResponse response = null;
		List<ExpandRow> resultRows = new ArrayList<ExpandRow>();
		List<TempRow> tempResultRows = new ArrayList<TempRow>();
		try {
			// sum字段的类型
			List<ByteString> sumListClas = new ArrayList<ByteString>();
			Scan scan = ProtobufUtil.toScan(request.getScan());
			// group By的字段
			List<ExpandCell> groupList = request.getGroupColumnsList();//在.proto文件中的ExpandAggregationRequest字段定义
			// count 的字段
			List<ExpandCell> countList = request.getCountColumnsList();
			// sum 的字段
			List<ExpandCell> sumList = request.getSumColumnsList();

			for (ExpandCell eCell : sumList) {
				sumListClas.add(eCell.getClassName());
			}
			scanner = env.getRegion().getScanner(scan);
			List<Cell> results = new ArrayList<Cell>();
			// qualifier can be null.
			boolean hasMoreRows = false;
			do {
				hasMoreRows = scanner.next(results);
				TempRow tempRow = new TempRow();
				for (Cell cell : results) {
					String family = Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(),
							cell.getFamilyLength());
					String qualif = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(),
							cell.getQualifierLength());
					for (ExpandCell group : groupList) {
						if (family.equals(group.getFamily().toStringUtf8())//internalsan扫出来的名字和request请求的名字一样
								&& qualif.equals(group.getQualify().toStringUtf8())) {
							String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(),
									cell.getValueLength());
							TempCell tempCell = new TempCell(family, qualif, value);
							tempRow.getKeys().add(tempCell);
							tempRow.getRow().add(tempCell);
						}
					}
				}
				int index = -1;
				for (int j = 0; j < tempResultRows.size(); j++) {
					List<TempCell> keys = tempResultRows.get(j).getKeys();
					List<TempCell> tempKeys = tempRow.getKeys();
					boolean mark = true;
					for (int i = 0; i < keys.size(); i++) {
						if (keys.get(i).getFamily().equals(tempKeys.get(i).getFamily())
								&& keys.get(i).getQualify().equals(tempKeys.get(i).getQualify())
								&& keys.get(i).getValue().equals(tempKeys.get(i).getValue())) {
							// index = j;
							// break;
							continue;
						} else {
							mark = false;
						}
					}
					if (mark)
						index = j;
					if (index > -1) {
						tempRow = tempResultRows.get(j);
						break;
					}
				}
				for (Cell cell : results) {
					String family = Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(),
							cell.getFamilyLength());
					String qualif = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(),
							cell.getQualifierLength());
					String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
					for (int i = 0; i < sumList.size(); i++) {
						ExpandCell sumCell = sumList.get(i);
						if (sumCell.getFamily().toStringUtf8().equals(family)
								&& sumCell.getQualify().toStringUtf8().equals(qualif)) {// 是sum例
							boolean sumCheck = false;// tempRow行中不存在
							List<TempCell> tempRowCell = tempRow.getRow();
							for (TempCell tempCell : tempRowCell) {
								if (tempCell.getFamily().equals(family) && tempCell.getQualify().equals(qualif)) {
									sumCheck = true;
									String tempValue = tempCell.getValue();
									tempValue = AddValue(value, tempValue, sumListClas.get(i));
									if (tempValue != null)
										tempCell.setValue(tempValue);
									else
										tempCell.setValue(0 + "");
									break;
								}
							}
							if (!sumCheck) {
								if (value == null || value.equals(""))
									tempRow.getRow().add(new TempCell(family, qualif, "0"));
								else
									tempRow.getRow().add(new TempCell(family, qualif, value));
							}
							break;
						}
					}
				}

				for (ExpandCell countCell : countList) {
					if (tempRow.getRow().size() > 0) {
						List<TempCell> tempCell = tempRow.getRow();
						boolean countCheck = false;
						for (TempCell tc : tempCell) {
							if (tc.getFamily().equals(countCell.getFamily().toStringUtf8())
									&& tc.getQualify().equals(countCell.getQualify().toStringUtf8())) {
								countCheck = true;
								String value = tc.getValue();
								value = AddValue(value, "1", ByteString.copyFromUtf8("Long"));
								if (value == null)
									tc.setValue("1");
								else
									tc.setValue(value);
								break;
							}
						}
						if (!countCheck) {
							tempRow.getRow().add(new TempCell(countCell.getFamily().toStringUtf8(),
									countCell.getQualify().toStringUtf8(), "1"));
						}
					}
				}

				if (index > -1) {
					tempResultRows.remove(index);
					tempResultRows.add(index, tempRow);
				} else
					tempResultRows.add(tempRow);
				results.clear();
			} while (hasMoreRows);

			if (tempResultRows.size() > 0) {
				for (TempRow tempRow : tempResultRows) {
					ExpandRow.Builder rowBuilder = ExpandRow.newBuilder();
					List<TempCell> tempKeys = tempRow.getKeys();
					List<TempCell> tempValues = tempRow.getRow();
					for (TempCell key : tempKeys) {
						ExpandCell.Builder cellBuilder = ExpandCell.newBuilder();
						cellBuilder.setFamily(ByteString.copyFromUtf8(key.getFamily()));
						cellBuilder.setQualify(ByteString.copyFromUtf8(key.getQualify()));
						cellBuilder.setValue(ByteString.copyFromUtf8(key.getValue()));
						rowBuilder.addKeys(cellBuilder.build());
					}
					for (TempCell v : tempValues) {
						ExpandCell.Builder cellBuilder = ExpandCell.newBuilder();
						cellBuilder.setFamily(ByteString.copyFromUtf8(v.getFamily()));
						cellBuilder.setQualify(ByteString.copyFromUtf8(v.getQualify()));
						cellBuilder.setValue(ByteString.copyFromUtf8(v.getValue()));
						rowBuilder.addValues(cellBuilder.build());
					}
					resultRows.add(rowBuilder.build());
				}
			}
			if (resultRows.size() > 0) {
				ExpandAggregationResponse.Builder responseBuilder = ExpandAggregationResponse.newBuilder();
				responseBuilder.addAllResults(resultRows);
				response = responseBuilder.build();
			}
		} catch (IOException e) {
			ResponseConverter.setControllerException(controller, e);
		} finally {
			if (scanner != null) {
				try {
					scanner.close();
				} catch (IOException ignored) {
				}
			}
		}
		// log.debug("group by size from this region is "
		// + env.getRegion().getRegionNameAsString() + ": " +
		// resultRows.size());
		done.run(response);
	}

	@Override
	public void getSumAndCount(RpcController controller, ExpandAggregationRequest request,
			RpcCallback<ExpandAggregationResponse> done) {
		// TODO Auto-generated method stub
		InternalScanner scanner = null;
		ExpandAggregationResponse response = null;
		List<ExpandRow> resultRows = new ArrayList<ExpandRow>();
		List<TempRow> tempResultRows = new ArrayList<TempRow>();
		try {
			// sum字段的类型
			List<ByteString> sumListClas = new ArrayList<ByteString>();
			Scan scan = ProtobufUtil.toScan(request.getScan());
			// count 的字段
			List<ExpandCell> countList = request.getCountColumnsList();
			// sum 的字段
			List<ExpandCell> sumList = request.getSumColumnsList();

			for (ExpandCell eCell : sumList) {
				sumListClas.add(eCell.getClassName());
			}
			scanner = env.getRegion().getScanner(scan);
			List<Cell> results = new ArrayList<Cell>();
			// qualifier can be null.
			boolean hasMoreRows = false;
			do {
				hasMoreRows = scanner.next(results);
				TempRow tempRow = null;
				if (tempResultRows.size() > 0) {
					tempRow = tempResultRows.get(0);
				} else
					tempRow = new TempRow();

				for (Cell cell : results) {
					String family = Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(),
							cell.getFamilyLength());
					String qualif = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(),
							cell.getQualifierLength());
					String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
					for (int i = 0; i < sumList.size(); i++) {
						ExpandCell sumCell = sumList.get(i);
						if (sumCell.getFamily().toStringUtf8().equals(family)
								&& sumCell.getQualify().toStringUtf8().equals(qualif)) {// 是sum例
							boolean sumCheck = false;// tempRow行中不存在
							List<TempCell> tempRowCell = tempRow.getRow();
							for (TempCell tempCell : tempRowCell) {
								if (tempCell.getFamily().equals(family) && tempCell.getQualify().equals(qualif)) {
									sumCheck = true;
									String tempValue = tempCell.getValue();
									tempValue = AddValue(value, tempValue, sumListClas.get(i));
									if (tempValue != null)
										tempCell.setValue(tempValue);
									else
										tempCell.setValue(0 + "");
									break;
								}
							}
							if (!sumCheck) {
								tempRow.getRow().add(new TempCell(family, qualif, value));
							}
							break;
						}
					}
				}

				for (ExpandCell countCell : countList) {
					if (tempRow.getRow().size() > 0) {
						List<TempCell> tempCell = tempRow.getRow();
						boolean countCheck = false;
						for (TempCell tc : tempCell) {
							if (tc.getFamily().equals(countCell.getFamily().toStringUtf8())
									&& tc.getQualify().equals(countCell.getQualify().toStringUtf8())) {
								countCheck = true;
								String value = tc.getValue();
								value = AddValue(value, "1", ByteString.copyFromUtf8("Long"));
								if (value == null)
									tc.setValue("1");
								else
									tc.setValue(value);
								break;
							}
						}
						if (!countCheck) {
							tempRow.getRow().add(new TempCell(countCell.getFamily().toStringUtf8(),
									countCell.getQualify().toStringUtf8(), "1"));
						}
					}
				}
				if (tempResultRows.size() > 0) {
					tempResultRows.remove(0);
				}
				tempResultRows.add(tempRow);
				results.clear();
			} while (hasMoreRows);

			if (tempResultRows.size() > 0) {
				for (TempRow tempRow : tempResultRows) {
					ExpandRow.Builder rowBuilder = ExpandRow.newBuilder();
					List<TempCell> tempValues = tempRow.getRow();
					for (TempCell v : tempValues) {
						ExpandCell.Builder cellBuilder = ExpandCell.newBuilder();
						cellBuilder.setFamily(ByteString.copyFromUtf8(v.getFamily()));
						cellBuilder.setQualify(ByteString.copyFromUtf8(v.getQualify()));
						cellBuilder.setValue(ByteString.copyFromUtf8(v.getValue()));
						rowBuilder.addValues(cellBuilder.build());
					}
					resultRows.add(rowBuilder.build());
				}
			}
			if (resultRows.size() > 0) {
				ExpandAggregationResponse.Builder responseBuilder = ExpandAggregationResponse.newBuilder();
				responseBuilder.addAllResults(resultRows);
				response = responseBuilder.build();
			}
		} catch (IOException e) {
			ResponseConverter.setControllerException(controller, e);
		} finally {
			if (scanner != null) {
				try {
					scanner.close();
				} catch (IOException ignored) {
				}
			}
		}

	}

	@Override
	public void getSumAndDistictCount(RpcController controller, ExpandAggregationRequest request,
			RpcCallback<ExpandAggregationResponse> done) {
		// TODO Auto-generated method stub

	}

	@Override
	public void getSum(RpcController controller, ExpandAggregationRequest request,
			RpcCallback<ExpandAggregationResponse> done) {
		InternalScanner scanner = null;
		ExpandAggregationResponse response = null;
		List<ExpandRow> resultRows = new ArrayList<ExpandRow>();
		List<TempRow> tempResultRows = new ArrayList<TempRow>();
		try {
			// sum字段的类型
			List<ByteString> sumListClas = new ArrayList<ByteString>();
			Scan scan = ProtobufUtil.toScan(request.getScan());
			// sum 的字段
			List<ExpandCell> sumList = request.getSumColumnsList();

			for (ExpandCell eCell : sumList) {
				sumListClas.add(eCell.getClassName());
			}
			scanner = env.getRegion().getScanner(scan);
			List<Cell> results = new ArrayList<Cell>();
			// qualifier can be null.
			boolean hasMoreRows = false;
			do {
				hasMoreRows = scanner.next(results);
				TempRow tempRow = null;
				if (tempResultRows.size() > 0) {
					tempRow = tempResultRows.get(0);
				} else
					tempRow = new TempRow();

				for (Cell cell : results) {
					String family = Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(),
							cell.getFamilyLength());
					String qualif = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(),
							cell.getQualifierLength());
					String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
					for (int i = 0; i < sumList.size(); i++) {
						ExpandCell sumCell = sumList.get(i);
						if (sumCell.getFamily().toStringUtf8().equals(family)
								&& sumCell.getQualify().toStringUtf8().equals(qualif)) {// 是sum例
							boolean sumCheck = false;// tempRow行中不存在
							List<TempCell> tempRowCell = tempRow.getRow();
							for (TempCell tempCell : tempRowCell) {
								if (tempCell.getFamily().equals(family) && tempCell.getQualify().equals(qualif)) {
									sumCheck = true;
									String tempValue = tempCell.getValue();
									tempValue = AddValue(value, tempValue, sumListClas.get(i));
									if (tempValue != null)
										tempCell.setValue(tempValue);
									else
										tempCell.setValue(0 + "");
									break;
								}
							}
							if (!sumCheck) {
								tempRow.getRow().add(new TempCell(family, qualif, value));
							}
							break;
						}
					}
				}

				if (tempResultRows.size() > 0) {
					tempResultRows.remove(0);
				}
				tempResultRows.add(tempRow);
				results.clear();
			} while (hasMoreRows);

			if (tempResultRows.size() > 0) {
				for (TempRow tempRow : tempResultRows) {
					ExpandRow.Builder rowBuilder = ExpandRow.newBuilder();
					List<TempCell> tempValues = tempRow.getRow();
					for (TempCell v : tempValues) {
						ExpandCell.Builder cellBuilder = ExpandCell.newBuilder();
						cellBuilder.setFamily(ByteString.copyFromUtf8(v.getFamily()));
						cellBuilder.setQualify(ByteString.copyFromUtf8(v.getQualify()));
						cellBuilder.setValue(ByteString.copyFromUtf8(v.getValue()));
						rowBuilder.addValues(cellBuilder.build());
					}
					resultRows.add(rowBuilder.build());
				}
			}
			if (resultRows.size() > 0) {
				ExpandAggregationResponse.Builder responseBuilder = ExpandAggregationResponse.newBuilder();
				responseBuilder.addAllResults(resultRows);
				response = responseBuilder.build();
			}
		} catch (IOException e) {
			ResponseConverter.setControllerException(controller, e);
		} finally {
			if (scanner != null) {
				try {
					scanner.close();
				} catch (IOException ignored) {
				}
			}
		}
	}

	@Override
	public void getCount(RpcController controller, ExpandAggregationRequest request,
			RpcCallback<ExpandAggregationResponse> done) {
		InternalScanner scanner = null;
		ExpandAggregationResponse response = null;
		List<ExpandRow> resultRows = new ArrayList<ExpandRow>();
		List<TempRow> tempResultRows = new ArrayList<TempRow>();
		try {
			Scan scan = ProtobufUtil.toScan(request.getScan());
			// count 的字段
			List<ExpandCell> countList = request.getCountColumnsList();
			scanner = env.getRegion().getScanner(scan);
			List<Cell> results = new ArrayList<Cell>();
			// qualifier can be null.
			boolean hasMoreRows = false;
			do {
				hasMoreRows = scanner.next(results);
				TempRow tempRow = null;
				if (tempResultRows.size() > 0) {
					tempRow = tempResultRows.get(0);
				} else
					tempRow = new TempRow();
				for (ExpandCell countCell : countList) {
					if (tempRow.getRow().size() > 0) {
						List<TempCell> tempCell = tempRow.getRow();
						boolean countCheck = false;
						for (TempCell tc : tempCell) {
							if (tc.getFamily().equals(countCell.getFamily().toStringUtf8())
									&& tc.getQualify().equals(countCell.getQualify().toStringUtf8())) {
								countCheck = true;
								String value = tc.getValue();
								value = AddValue(value, "1", ByteString.copyFromUtf8("Long"));
								if (value == null)
									tc.setValue("1");
								else
									tc.setValue(value);
								break;
							}
						}
						if (!countCheck) {
							tempRow.getRow().add(new TempCell(countCell.getFamily().toStringUtf8(),
									countCell.getQualify().toStringUtf8(), "1"));
						}
					} else {
						tempRow.getRow().add(new TempCell(countCell.getFamily().toStringUtf8(),
								countCell.getQualify().toStringUtf8(), "1"));
					}
				}
				if (tempResultRows.size() > 0) {
					tempResultRows.remove(0);
				}
				tempResultRows.add(tempRow);
				results.clear();
			} while (hasMoreRows);

			if (tempResultRows.size() > 0) {
				for (TempRow tempRow : tempResultRows) {
					ExpandRow.Builder rowBuilder = ExpandRow.newBuilder();
					List<TempCell> tempValues = tempRow.getRow();
					for (TempCell v : tempValues) {
						ExpandCell.Builder cellBuilder = ExpandCell.newBuilder();
						cellBuilder.setFamily(ByteString.copyFromUtf8(v.getFamily()));
						cellBuilder.setQualify(ByteString.copyFromUtf8(v.getQualify()));
						cellBuilder.setValue(ByteString.copyFromUtf8(v.getValue()));
						rowBuilder.addValues(cellBuilder.build());
					}
					resultRows.add(rowBuilder.build());
				}
			}
			if (resultRows.size() > 0) {
				ExpandAggregationResponse.Builder responseBuilder = ExpandAggregationResponse.newBuilder();
				responseBuilder.addAllResults(resultRows);
				response = responseBuilder.build();
			}
		} catch (IOException e) {
			ResponseConverter.setControllerException(controller, e);
		} finally {
			if (scanner != null) {
				try {
					scanner.close();
				} catch (IOException ignored) {
				}
			}
		}
	}

	@Override
	public void getDistictCount(RpcController controller, ExpandAggregationRequest request,
			RpcCallback<ExpandAggregationResponse> done) {
		InternalScanner scanner = null;
		ExpandAggregationResponse response = null;
		List<ExpandRow> resultRows = new ArrayList<ExpandRow>();
		List<TempRow> tempResultRows = new ArrayList<TempRow>();
		try {
			Scan scan = ProtobufUtil.toScan(request.getScan());
			// count 的字段
			List<ExpandCell> distinctCountList = request.getDistictColumnsList();

			scanner = env.getRegion().getScanner(scan);
			List<Cell> results = new ArrayList<Cell>();
			// qualifier can be null.
			boolean hasMoreRows = false;
			do {
				hasMoreRows = scanner.next(results);
				TempRow tempRow = null;
				if (tempResultRows.size() > 0) {
					tempRow = tempResultRows.get(0);
				} else {
					tempRow = new TempRow();
				}
				for (Cell cell : results) {
					String family = Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(),
							cell.getFamilyLength());
					String qualif = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(),
							cell.getQualifierLength());
					String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(),
							cell.getQualifierLength());
					for (ExpandCell eCell : distinctCountList) {
						String eFamily = eCell.getFamily().toStringUtf8();
						String eQualif = eCell.getQualify().toStringUtf8();
						if (family.equals(eFamily) && qualif.equals(eQualif)) {// 是同一列
							if (tempRow.getRow().size() > 0) {
								List<TempCell> tempCells = tempRow.getRow();
								boolean mark = false;
								for (TempCell tcell : tempCells) {
									if (tcell.getFamily().equals(family) && tcell.getQualify().equals(qualif)) {
										mark = true;
										List<String> tempValues = tcell.getValues();
										boolean flag = false;
										for (String v : tempValues) {
											if ((v == null && value == null) || (v != null && v.equals(value))) {
												flag = true;
												break;
											}
										}
										if (!flag) {
											tcell.getValues().add(value);
											break;
										}
									}
								}
								if (!mark) {
									TempCell tempCell = new TempCell(family, qualif, value, true);
									tempRow.getRow().add(tempCell);
									break;
								}
							} else {
								TempCell tempCell = new TempCell(family, qualif, value, true);
								tempRow.getRow().add(tempCell);
							}
						}
					}
				}
				if (tempResultRows.size() > 0) {
					tempResultRows.remove(0);
				}
				tempResultRows.add(tempRow);
				results.clear();
			} while (hasMoreRows);

			for (TempRow row : tempResultRows) {
				ExpandRow.Builder expandRow = ExpandRow.newBuilder();
				List<TempCell> tempCell = row.getRow();
				for (TempCell v : tempCell) {
					ExpandCell.Builder cellBuilder = ExpandCell.newBuilder();
					cellBuilder.setFamily(ByteString.copyFromUtf8(v.getFamily()));
					cellBuilder.setQualify(ByteString.copyFromUtf8(v.getQualify()));
					for (String s : v.getValues()) {
						if (s == null) {
							cellBuilder.addDistinctValues(ByteString.copyFromUtf8(""));
						} else {
							cellBuilder.addDistinctValues(ByteString.copyFromUtf8(s));
						}
					}
					expandRow.addValues(cellBuilder.build());
				}
				resultRows.add(expandRow.build());
			}
			if (resultRows.size() > 0) {
				ExpandAggregationResponse.Builder responseBuilder = ExpandAggregationResponse.newBuilder();
				responseBuilder.addAllResults(resultRows);
				response = responseBuilder.build();
			}
		} catch (IOException e) {
			ResponseConverter.setControllerException(controller, e);
		} finally {
			if (scanner != null) {
				try {
					scanner.close();
				} catch (IOException ignored) {
				}
			}
		}
	}

	@Override
	public void getCountAndDistictCount(RpcController controller, ExpandAggregationRequest request,
			RpcCallback<ExpandAggregationResponse> done) {
		// TODO Auto-generated method stub

	}

	@Override
	public void getGroupAndSum(RpcController controller, ExpandAggregationRequest request,
			RpcCallback<ExpandAggregationResponse> done) {
		// TODO Auto-generated method stub
		InternalScanner scanner = null;
		ExpandAggregationResponse response = null;
		List<ExpandRow> resultRows = new ArrayList<ExpandRow>();
		List<TempRow> tempResultRows = new ArrayList<TempRow>();
		try {
			// sum字段的类型
			List<ByteString> sumListClas = new ArrayList<ByteString>();
			Scan scan = ProtobufUtil.toScan(request.getScan());
			// group By的字段
			List<ExpandCell> groupList = request.getGroupColumnsList();
			// sum 的字段
			List<ExpandCell> sumList = request.getSumColumnsList();

			for (ExpandCell eCell : sumList) {
				sumListClas.add(eCell.getClassName());
			}
			scanner = env.getRegion().getScanner(scan);
			List<Cell> results = new ArrayList<Cell>();
			// qualifier can be null.
			boolean hasMoreRows = false;
			do {
				hasMoreRows = scanner.next(results);
				TempRow tempRow = new TempRow();
				for (Cell cell : results) {
					String family = Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(),
							cell.getFamilyLength());
					String qualif = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(),
							cell.getQualifierLength());
					for (ExpandCell group : groupList) {
						if (family.equals(group.getFamily().toStringUtf8())
								&& qualif.equals(group.getQualify().toStringUtf8())) {
							String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(),
									cell.getValueLength());
							TempCell tempCell = new TempCell(family, qualif, value);
							tempRow.getKeys().add(tempCell);
							tempRow.getRow().add(tempCell);
						}
					}
				}
				int index = -1;
				for (int j = 0; j < tempResultRows.size(); j++) {
					List<TempCell> keys = tempResultRows.get(j).getKeys();
					List<TempCell> tempKeys = tempRow.getKeys();
					boolean mark = true;
					for (int i = 0; i < keys.size(); i++) {
						if (keys.get(i).getFamily().equals(tempKeys.get(i).getFamily())
								&& keys.get(i).getQualify().equals(tempKeys.get(i).getQualify())
								&& keys.get(i).getValue().equals(tempKeys.get(i).getValue())) {
							// index = j;
							// break;
							continue;
						} else {
							mark = false;
						}
					}
					if (mark)
						index = j;
					if (index > -1) {
						tempRow = tempResultRows.get(j);
						break;
					}
				}
				for (Cell cell : results) {
					String family = Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(),
							cell.getFamilyLength());
					String qualif = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(),
							cell.getQualifierLength());
					String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
					for (int i = 0; i < sumList.size(); i++) {
						ExpandCell sumCell = sumList.get(i);
						if (sumCell.getFamily().toStringUtf8().equals(family)
								&& sumCell.getQualify().toStringUtf8().equals(qualif)) {// 是sum例
							boolean sumCheck = false;// tempRow行中不存在
							List<TempCell> tempRowCell = tempRow.getRow();
							for (TempCell tempCell : tempRowCell) {
								if (tempCell.getFamily().equals(family) && tempCell.getQualify().equals(qualif)) {
									sumCheck = true;
									String tempValue = tempCell.getValue();
									tempValue = AddValue(value, tempValue, sumListClas.get(i));
									if (tempValue != null)
										tempCell.setValue(tempValue);
									else
										tempCell.setValue(0 + "");
									break;
								}
							}
							if (!sumCheck) {
								tempRow.getRow().add(new TempCell(family, qualif, value));
							}
							break;
						}
					}
				}

				if (index > -1) {
					tempResultRows.remove(index);
					tempResultRows.add(index, tempRow);
				} else
					tempResultRows.add(tempRow);
				results.clear();
			} while (hasMoreRows);

			if (tempResultRows.size() > 0) {
				for (TempRow tempRow : tempResultRows) {
					ExpandRow.Builder rowBuilder = ExpandRow.newBuilder();
					List<TempCell> tempKeys = tempRow.getKeys();
					List<TempCell> tempValues = tempRow.getRow();
					for (TempCell key : tempKeys) {
						ExpandCell.Builder cellBuilder = ExpandCell.newBuilder();
						cellBuilder.setFamily(ByteString.copyFromUtf8(key.getFamily()));
						cellBuilder.setQualify(ByteString.copyFromUtf8(key.getQualify()));
						cellBuilder.setValue(ByteString.copyFromUtf8(key.getValue()));
						rowBuilder.addKeys(cellBuilder.build());
					}
					for (TempCell v : tempValues) {
						ExpandCell.Builder cellBuilder = ExpandCell.newBuilder();
						cellBuilder.setFamily(ByteString.copyFromUtf8(v.getFamily()));
						cellBuilder.setQualify(ByteString.copyFromUtf8(v.getQualify()));
						cellBuilder.setValue(ByteString.copyFromUtf8(v.getValue()));
						rowBuilder.addValues(cellBuilder.build());
					}
					resultRows.add(rowBuilder.build());
				}
			}
			if (resultRows.size() > 0) {
				ExpandAggregationResponse.Builder responseBuilder = ExpandAggregationResponse.newBuilder();
				responseBuilder.addAllResults(resultRows);
				response = responseBuilder.build();
			}
		} catch (IOException e) {
			ResponseConverter.setControllerException(controller, e);
		} finally {
			if (scanner != null) {
				try {
					scanner.close();
				} catch (IOException ignored) {
				}
			}
		}
	}

	@Override
	public void getGroupAndCount(RpcController controller, ExpandAggregationRequest request,
			RpcCallback<ExpandAggregationResponse> done) {
		InternalScanner scanner = null;
		ExpandAggregationResponse response = null;
		List<ExpandRow> resultRows = new ArrayList<ExpandRow>();
		List<TempRow> tempResultRows = new ArrayList<TempRow>();
		try {
			Scan scan = ProtobufUtil.toScan(request.getScan());
			List<ExpandCell> groupList = request.getGroupColumnsList();// group
																		// by字段
			List<ExpandCell> countList = request.getGroupColumnsList();// count
			scanner = env.getRegion().getScanner(scan);
			List<Cell> results = new ArrayList<Cell>();
			boolean hasMoreRows = false;
			do {
				hasMoreRows = scanner.next(results);
				TempRow tempRow = new TempRow();
				for (Cell cell : results) {
					String family = Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(),
							cell.getFamilyLength());
					String qualif = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(),
							cell.getQualifierLength());
					for (ExpandCell group : groupList) {
						if (family.equals(group.getFamily().toStringUtf8())
								&& qualif.equals(group.getQualify().toStringUtf8())) {
							String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(),
									cell.getValueLength());
							TempCell tempCell = new TempCell(family, qualif, value);
							tempRow.getKeys().add(tempCell);
							tempRow.getRow().add(tempCell);
							// 我自己加的，原文里没有
							// tempResultRows.add(tempRow);
						}
					}
				}
				int index = -1;
				for (int j = 0; j < tempResultRows.size(); j++) {
					List<TempCell> keys = tempResultRows.get(j).getKeys();
					List<TempCell> tempKeys = tempRow.getKeys();
					boolean mark = true;
					for (int i = 0; i < keys.size(); i++) {
						if (keys.get(i).getFamily().equals(tempKeys.get(i).getFamily())
								&& keys.get(i).getQualify().equals(tempKeys.get(i).getQualify())
								&& keys.get(i).getValue().equals(tempKeys.get(i).getValue())) {
							continue;
						} else {
							mark = false;
						}
					}
					if (mark) {
						index = j;
					}
					if (index > -1) {
						tempRow = tempResultRows.get(j);
						break;
					}
				}
				for (ExpandCell countCell : countList) {
					if (tempRow.getRow().size() > 0) {
						List<TempCell> tempCell = tempRow.getRow();
						boolean countCheck = false;
						for (TempCell tc : tempCell) {
							if (tc.getFamily().equals(countCell.getFamily().toStringUtf8())
									&& tc.getQualify().equals(countCell.getQualify().toStringUtf8())) {
								countCheck = true;
								String value = tc.getValue();
								value = AddValue(value, "1", ByteString.copyFromUtf8("Long"));
								if (value == null)
									tc.setValue("1");
								else
									tc.setValue(value);
								break;
							}
						}
						if (!countCheck) {
							tempRow.getRow().add(new TempCell(countCell.getFamily().toStringUtf8(),
									countCell.getQualify().toStringUtf8(), "1"));
						}
					}
				}
				if (index > -1) {
					tempResultRows.remove(index);
					tempResultRows.add(index, tempRow);
				} else
					tempResultRows.add(tempRow);
				results.clear();
			} while (hasMoreRows);

			if (tempResultRows.size() > 0) {
				for (TempRow tempRow : tempResultRows) {
					ExpandRow.Builder rowBuilder = ExpandRow.newBuilder();
					List<TempCell> tempKeys = tempRow.getKeys();
					List<TempCell> tempValues = tempRow.getRow();
					for (TempCell key : tempKeys) {
						ExpandCell.Builder cellBuilder = ExpandCell.newBuilder();
						cellBuilder.setFamily(ByteString.copyFromUtf8(key.getFamily()));
						cellBuilder.setQualify(ByteString.copyFromUtf8(key.getQualify()));
						cellBuilder.setValue(ByteString.copyFromUtf8(key.getValue()));
						rowBuilder.addKeys(cellBuilder.build());
					}
					for (TempCell v : tempValues) {
						ExpandCell.Builder cellBuilder = ExpandCell.newBuilder();
						cellBuilder.setFamily(ByteString.copyFromUtf8(v.getFamily()));
						cellBuilder.setQualify(ByteString.copyFromUtf8(v.getQualify()));
						cellBuilder.setValue(ByteString.copyFromUtf8(v.getValue()));
						rowBuilder.addValues(cellBuilder.build());
					}
					resultRows.add(rowBuilder.build());
				}
			}
			if (resultRows.size() > 0) {
				ExpandAggregationResponse.Builder responseBuilder = ExpandAggregationResponse.newBuilder();
				responseBuilder.addAllResults(resultRows);
				response = responseBuilder.build();
			}
		} catch (Exception e) {
			// TODO: handle exception

		} finally {
			if (scanner != null) {
				try {
					scanner.close();
				} catch (IOException ignored) {
				}
			}
		}
	}

	@Override
	public void getGroupAndDistictCount(RpcController controller, ExpandAggregationRequest request,
			RpcCallback<ExpandAggregationResponse> done) {
		// TODO Auto-generated method stub

	}

	@Override
	public void getGroupAndDistictCountAndCount(RpcController controller, ExpandAggregationRequest request,
			RpcCallback<ExpandAggregationResponse> done) {
		// TODO Auto-generated method stub

	}

	public void start(CoprocessorEnvironment env) throws IOException {
		// TODO Auto-generated method stub
		if (env instanceof RegionCoprocessorEnvironment) {
			this.env = (RegionCoprocessorEnvironment) env;
		} else {
			throw new CoprocessorException("Must be loaded on a table region!");
		}
	}

	public void stop(CoprocessorEnvironment env) throws IOException {
		// TODO Auto-generated method stub

	}

	public Service getService() {
		// TODO Auto-generated method stub
		return this;
	}

	String AddValue(String v1, String v2, ByteString className) {
		String c = className.toStringUtf8();
		if ("long".equals(c) || "Long".equals(c)) {
			Long l1 = null;
			Long l2 = null;
			if (v1 != null && !v1.equals(""))
				l1 = Long.parseLong(v1);
			if (v2 != null && !v2.equals(""))
				l2 = Long.parseLong(v2);
			if (l1 == null ^ l2 == null) {
				return (((l1 == null) ? l2 : l1)) + ""; // either of one is
														// null.
			} else if (l1 == null) // both are null
				return null;
			return (l1 + l2) + "";
		} else if ("int".equals(c) || "Integer".equals(c)) {
			Integer i1 = null;
			Integer i2 = null;
			if (v1 != null && !v1.equals(""))
				i1 = Integer.parseInt(v1);
			if (v2 != null && !v2.equals(""))
				i2 = Integer.parseInt(v2);
			if (i1 == null ^ i2 == null) {
				return ((i1 == null) ? i2 : i1) + ""; // either of one is null.
			} else if (i1 == null) {
				return null;
			}
			return (i1 + i2) + "";
		} else if ("double".equals(c) || "Double".equals(c)) {
			Double d1 = null;
			Double d2 = null;
			if (v1 != null && !v1.equals(""))
				d1 = Double.parseDouble(v1);
			if (v2 != null && !v2.equals(""))
				d2 = Double.parseDouble(v2);
			if (d1 == null ^ d2 == null) {
				return ((d1 == null) ? d2 : d1) + ""; // either of one is null.
			} else if (d1 == null) {
				return null;
			}
			return (d1 + d2) + "";
		} else {
			BigDecimal b1 = null;
			BigDecimal b2 = null;
			if (v1 != null && !v1.equals(""))
				b1 = BigDecimal.valueOf(Double.parseDouble(v1));
			if (v2 != null && !v2.equals(""))
				b2 = BigDecimal.valueOf(Double.parseDouble(v2));
			if (b1 == null ^ b2 == null) {
				return ((b1 == null) ? b2 : b1) + ""; // either of one is null.
			}
			if (b1 == null) {
				return null;
			}
			return b1.add(b2).toString();
		}
	}

	static class TempCell {
		private String family;
		private String qualify;
		private String value;
		private List<String> values = new ArrayList<String>();

		public String getFamily() {
			return family;
		}

		public void setFamily(String family) {
			this.family = family;
		}

		public String getQualify() {
			return qualify;
		}

		public void setQualify(String qualify) {
			this.qualify = qualify;
		}

		public String getValue() {
			return value;
		}

		public void setValue(String value) {
			this.value = value;
		}

		public List<String> getValues() {
			return values;
		}

		public void setValues(List<String> values) {
			this.values = values;
		}

		public TempCell(String family, String qualify) {
			this.family = family;
			this.qualify = qualify;
		}

		public TempCell(String family, String qualify, String value) {
			this.family = family;
			this.qualify = qualify;
			this.value = value;
		}

		public TempCell(String family, String qualify, String value, boolean flag) {
			this.family = family;
			this.qualify = qualify;
			this.values.add(value);
		}

	}

	static class TempRow {
		private List<TempCell> keys = new ArrayList<TempCell>();
		private List<TempCell> row = new ArrayList<TempCell>();

		public List<TempCell> getKeys() {
			return keys;
		}

		public void setKeys(List<TempCell> keys) {
			this.keys = keys;
		}

		public List<TempCell> getRow() {
			return row;
		}

		public void setRow(List<TempCell> row) {
			this.row = row;
		}

	}
}
