package com.jdvn.setl.geos.processors.db;

import com.cci.gss.GSSConstants;


public class LayerMetadata {
	

	public int mThemeId;
	

	public int mSrId;
	

	public String mThemeTableCatalog;
	

	public String mThemeTableSchema;
	

	public String mThemeTableName;
	

	public double mMinX;
	

	public double mMinY;


	public double mMaxX;
	

	public double mMaxY;
	

	public int mBitEncodeValue;
	

	public double mGridSize;
	
	public int mScaleFactor;
	

	public String mGeometryColumn;
	

	public String mGeometryTableCatalog;
	

	public String mGeometryTableSchema;
	

	public String mGeometryTableName;
	

	public int mGeometryType;
	

	public int mStorageType;
	

	public String mCrs;
	

	public int mViewLink;
	

	public String getUniqueIdColumn() {
		return mGeometryColumn;
	}
	

	public String getFeatureTableFullName() {
		return mThemeTableName;
	}
	

	public String getGeometryTableFullName() {
		return mGeometryTableName;
	}
	

	public double getAppliedGridSize() {
		return mGridSize;
	}
	

	public String getIndexTableFullName() {
		if (mViewLink > 0) {
			return "X" + mViewLink;
		}
		else {
			return "X" + mThemeId;
		}
	}
	

	public String getIdColumnInGeometryTable() {
		return "GID";
	}
	

	public String getGeometryColumnInGeometryTable() {
		return "GEOMETRY";
	}
	

	public boolean isEditable() {
		return mViewLink <= 0;// && fBitEncodeValue != GSSConstants.LAYER_NETWORK_SELF;
	}
	

	public boolean isViewLayer() {
		return mViewLink > 0;
	}
	

	public boolean isCompatibleType(int geometryType) {
		if (mGeometryType == geometryType) {
			return true;
		}
		
		if (mGeometryType == GSSConstants.TYPE_POINT) {
			return geometryType == GSSConstants.TYPE_MULTI_POINT;
		}
		else if (mGeometryType == GSSConstants.TYPE_LINESTRING) {
			return geometryType == GSSConstants.TYPE_MULTI_LINESTRING;
		}
		else {
			return geometryType == GSSConstants.TYPE_MULTI_POLYGON;
		}
	}

}
