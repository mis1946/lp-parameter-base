/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package org.rmj.lp.parameter.base;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.RowSetFactory;
import javax.sql.rowset.RowSetProvider;
import org.json.simple.JSONObject;
import org.rmj.appdriver.GRider;
import org.rmj.appdriver.MiscUtil;
import org.rmj.appdriver.SQLUtil;
import org.rmj.appdriver.agentfx.CommonUtils;
import org.rmj.appdriver.agentfx.ui.showFXDialog;
import org.rmj.appdriver.constants.EditMode;
import org.rmj.appdriver.constants.RecordStatus;
import org.rmj.appdriver.constants.UserRight;
import org.rmj.cas.client.application.ClientFX;

/**
 *
 * @author Maynard
 */
public class ARDelivery {

    private final String MASTER_TABLE = "Delivery_Service";
    private final String ARMASTER_TABLE = "AR_Master";
    private final String HISTORY_TABLE = "Delivery_Service_Charge_History";
    private final GRider p_oApp;
    private final boolean p_bWithParent;
    private String p_sBranchCd;
    private int p_nEditMode;
    private int p_nTranStat;
    private String p_sMessage;
    private boolean p_bWithUI = true;

    private boolean p_bSkipMaster = false;
    private CachedRowSet p_oMaster;
    private CachedRowSet p_oARecMaster;

    private String p_oOldDate;

    private LMasDetTrans p_oListener;

    public ARDelivery(GRider foApp, String fsBranchCd, boolean fbWithParent) {
        p_oApp = foApp;
        p_sBranchCd = fsBranchCd;
        p_bWithParent = fbWithParent;

        if (p_sBranchCd.isEmpty()) {
            p_sBranchCd = p_oApp.getBranchCode();
        }

        p_nEditMode = EditMode.UNKNOWN;
    }

    public void setTranStat(int fnValue) {
        p_nTranStat = fnValue;
    }

    public void setListener(LMasDetTrans foValue) {
        p_oListener = foValue;
    }

    public void setWithUI(boolean fbValue) {
        p_bWithUI = fbValue;
    }

    public int getEditMode() {
        return p_nEditMode;
    }

    public String getMessage() {
        return p_sMessage;
    }

    private int getColumnIndex(CachedRowSet loRS, String fsValue) throws SQLException {
        int lnIndex = 0;
        int lnRow = loRS.getMetaData().getColumnCount();

        for (int lnCtr = 1; lnCtr <= lnRow; lnCtr++) {
            if (fsValue.equals(loRS.getMetaData().getColumnLabel(lnCtr))) {
                lnIndex = lnCtr;
                break;
            }
        }
        return lnIndex;
    }

    public int getItemCount() throws SQLException {
        if (p_oARecMaster == null) {
            return 0;
        }
        p_oARecMaster.last();
        return p_oARecMaster.getRow();
    }

    public Object getARDetail(int fnRow, int fnIndex) throws SQLException {
        if (fnIndex == 0) {
            return null;
        }
        if (getItemCount() == 0 || fnRow > getItemCount()) {
            return null;
        }
        p_oARecMaster.absolute(fnRow);
        return p_oARecMaster.getObject(fnIndex);
    }

    public Object getARDetail(int fnRow, String fsIndex) throws SQLException {
        return getARDetail(fnRow, getColumnIndex(p_oARecMaster, fsIndex));
    }

    public void setARDetail(int fnRow, int fnIndex, Object foValue) throws SQLException {
        if (getItemCount() == 0 || fnRow > getItemCount()) {
            return;
        }
        p_oARecMaster.absolute(fnRow);
        switch (fnIndex) {
            case 4://sCPerson1
            case 5://sCPPosit1
            case 6://sTelNoxxx
            case 7://sFaxNoxxx
            case 8://sRemarksx
                p_oARecMaster.updateString(fnIndex, ((String) foValue).trim());
                break;
            case 10://nDisCount
            case 11://nCredLimt
            case 12://nABalance
            case 13://nOBalance
            case 14://nBalForwd
                if (foValue instanceof Double) {
                    p_oARecMaster.updateObject(fnIndex, foValue);
                }
                break;
            case 15://dBalForwd
            case 16://dCltSince
                if (foValue instanceof Date) {
                    p_oARecMaster.updateObject(fnIndex, foValue);
                }
                break;
            case 18://cHoldAcct
                if (foValue instanceof Integer) {
                    p_oARecMaster.updateObject(fnIndex, foValue);
                }
                break;
        }
        p_oARecMaster.updateRow();
        if (p_oListener != null) {
            p_oListener.DetailRetreive(fnRow, fnIndex, p_oARecMaster.getObject(fnIndex));
        }
    }

    public void setARDetail(int fnRow, String fsIndex, Object foValue) throws SQLException {
        setARDetail(fnRow, getColumnIndex(p_oARecMaster, fsIndex), foValue);
    }

    public Object getMaster(int fnIndex) throws SQLException {
        if (fnIndex == 0) {
            return null;
        }
        p_oMaster.first();
        return p_oMaster.getObject(fnIndex);
    }

    public Object getMaster(String fsIndex) throws SQLException {
        return getMaster(getColumnIndex(p_oMaster, fsIndex));
    }

    public void setMaster(int fnIndex, Object foValue) throws SQLException {
        p_oMaster.first();
        switch (fnIndex) {
            case 2://dPartnerx
            case 5://dSrvcChrg
                if (foValue instanceof Date) {
                    p_oMaster.updateObject(fnIndex, foValue);
                }
                break;
            case 3://sBriefDsc
            case 4://sDescript
                p_oMaster.updateString(fnIndex, ((String) foValue).trim());
                break;
            case 6://nSrvcChrg
                if (foValue instanceof Double) {
                    p_oMaster.updateObject(fnIndex, foValue);
                }
                break;
        }
        p_oMaster.updateRow();
        if (p_oListener != null) {
            p_oListener.MasterRetreive(fnIndex, p_oMaster.getObject(fnIndex));
        }
    }

    public void setMaster(String fsIndex, Object foValue) throws SQLException {
        setMaster(getColumnIndex(p_oMaster, fsIndex), foValue);

    }

    public String getSQ_Master() {
        String lsSQL;

        lsSQL = "SELECT "
                + " a.sRiderIDx sRiderIDx "
                + " , a.dPartnerx dPartnerx "
                + " , a.sBriefDsc sBriefDsc "
                + " , a.sDescript sDescript "
                + " , a.dSrvcChrg dSrvcChrg "
                + " , a.nSrvcChrg nSrvcChrg "
                + " , a.sClientID sClientID "
                + " , a.cRecdStat cRecdStat "
                + " , a.sModified sModified "
                + " , a.dModified dModified "
                + " FROM " + MASTER_TABLE + " a ";

        return lsSQL;

    }

    public String getSQ_Detail() {
        String lsSQL;

        lsSQL = "SELECT "
                + " a.sClientID sClientID "
                + " , a.sBranchCd sBranchCd "
                + " , c.sBranchNm sBranchNm "
                + " , a.sCPerson1 sCPerson1 "
                + " , a.sCPPosit1 sCPPosit1 "
                + " , a.sTelNoxxx sTelNoxxx "
                + " , a.sFaxNoxxx sFaxNoxxx "
                + " , a.sRemarksx sRemarksx "
                + " , a.sTermIDxx sTermIDxx "
                + " , a.nDisCount nDisCount "
                + " , a.nCredLimt nCredLimt "
                + " , a.nABalance nABalance "
                + " , a.nOBalance nOBalance "
                + " , a.nBalForwd nBalForwd "
                + " , a.dBalForwd dBalForwd "
                + " , a.dCltSince dCltSince "
                + " , a.cAutoHold cAutoHold "
                + " , a.cHoldAcct cHoldAcct "
                + " , a.cRecdStat cRecdStat "
                + " , a.dTimeStmp dTimeStmp "
                + " , b.sCompnyNm sCompnyNm "
                + " , b.sLastName sLastName "
                + " , b.sFrstName sFrstName "
                + " , b.sMiddName sMiddName "
                + " , b.sSuffixNm sSuffixNm "
                + " , CONCAT(e.sHouseNox, ' ', e.sAddressx, ', ', f.sTownName, ', ', g.sProvName) xAddressx"
                + " , d.sDescript xTermName "
                + " FROM " + ARMASTER_TABLE + " a "
                + " LEFT JOIN Client_Master b "
                + " ON a.sClientID = b.sClientID "
                + " LEFT JOIN Branch c "
                + "  ON a.sBranchCd = c.sBranchCd "
                + " LEFT JOIN Term d "
                + "  ON a.sTermIDxx = d.sTermCode "
                + " LEFT JOIN Client_Address e"
                + " ON a.sClientID = e.sClientID"
                + " AND e.nPriority = 1"
                + " LEFT JOIN TownCity f"
                + " ON e.sTownIDxx = f.sTownIDxx"
                + " LEFT JOIN Province g"
                + " ON f.sProvIDxx = g.sProvIDxx";

        return lsSQL;

    }

    public String getSQ_Branch() {
        String lsSQL = "";

        lsSQL = "SELECT"
                + "  sBranchCd"
                + ", sBranchNm"
                + " FROM Branch a"
                + " WHERE cRecdStat = 1";

        return lsSQL;
    }

    public String getSQ_DSChargeHistory() {
        String lsSQL = "";

        lsSQL = "SELECT"
                + "  sRiderIDx"
                + ", dSrvcChrg"
                + ", nSrvcChrg"
                + ", dTimeStmp"
                + " FROM Delivery_Service_Charge_History ";

        return lsSQL;
    }

    public boolean searchBranch(int fnRow, String fsValue, boolean fbByCode) throws SQLException {

        String lsSQL = getSQ_Branch();
        if (fbByCode) {
            lsSQL = MiscUtil.addCondition(lsSQL, "sBranchCd = " + SQLUtil.toSQL(fsValue));
        } else {
            lsSQL = MiscUtil.addCondition(lsSQL, "sBranchNm LIKE " + SQLUtil.toSQL(fsValue + "%"));
        }

        JSONObject loJSON;

        if (p_bWithUI) {
            loJSON = showFXDialog.jsonSearch(
                    p_oApp,
                    lsSQL,
                    fsValue,
                    "Code»Branch Name",
                    "sBranchCd»sBranchNm",
                    "sBranchCd»sBranchNm",
                    fbByCode ? 0 : 1);

            if (loJSON != null) {
                p_oARecMaster.absolute(fnRow);
                p_oARecMaster.updateString("sBranchCd", (String) loJSON.get("sBranchCd"));
                p_oARecMaster.updateString("sBranchNm", (String) loJSON.get("sBranchNm"));
                p_oARecMaster.updateRow();
                return true;
            } else {
                p_sMessage = "No record selected.";
                return false;
            }
        }

        return true;
    }

    public boolean NewTransaction() throws SQLException {
        if (p_oApp == null) {
            p_sMessage = "Application driver is not set.";
            return false;
        }
        p_nEditMode = EditMode.UNKNOWN;

        p_sMessage = "";
        String lsSQL;
        ResultSet loRS;
        RowSetFactory factory = RowSetProvider.newFactory();

        //createcacherowset for master
        lsSQL = MiscUtil.addCondition(getSQ_Master(), "0=1");
        loRS = p_oApp.executeQuery(lsSQL);
        p_oMaster = factory.createCachedRowSet();
        p_oMaster.populate(loRS);
        MiscUtil.close(loRS);

        //setobject required
        initMaster();

        //createcacherowset for master
        lsSQL = MiscUtil.addCondition(getSQ_Detail(), "0=1");
        loRS = p_oApp.executeQuery(lsSQL);
        p_oARecMaster = factory.createCachedRowSet();
        p_oARecMaster.populate(loRS);
        MiscUtil.close(loRS);
        initDetail();

        p_nEditMode = EditMode.ADDNEW;
        return true;
    }

    private void initMaster() {
        try {
            p_oMaster.last();
            p_oMaster.moveToInsertRow();

            MiscUtil.initRowSet(p_oMaster);
            p_oMaster.updateObject("sRiderIDx", MiscUtil.getNextCode(MASTER_TABLE, "sRiderIDx", false, p_oApp.getConnection(), ""));
            p_oMaster.updateObject("dPartnerx", p_oApp.getServerDate());
            p_oMaster.updateObject("dModified", p_oApp.getServerDate());
            p_oMaster.updateObject("sModified", p_oApp.getUserID());
            p_oMaster.updateObject("cRecdStat", RecordStatus.ACTIVE);
            p_oMaster.updateObject("nSrvcChrg", 0.00);
            p_oMaster.updateObject("dSrvcChrg", p_oApp.getServerDate());
            p_oMaster.insertRow();
            p_oMaster.moveToCurrentRow();

        } catch (SQLException ex) {
            Logger.getLogger(ARDelivery.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    private void initDetail() throws SQLException {

        p_oARecMaster.last();
        p_oARecMaster.moveToInsertRow();

        MiscUtil.initRowSet(p_oARecMaster);
        p_oARecMaster.updateInt("cRecdStat", 1);
        p_oARecMaster.updateInt("cHoldAcct", 0);
        p_oARecMaster.updateInt("cAutoHold", 0);
        p_oARecMaster.updateString("sCompnyNm", "");
        p_oARecMaster.updateString("sBranchNm", "");
        p_oARecMaster.updateObject("nDisCount", 0.00);
        p_oARecMaster.updateObject("nCredLimt", 0.00);
        p_oARecMaster.updateObject("dBalForwd", p_oApp.getServerDate());
        p_oARecMaster.updateObject("nBalForwd", 0.00);
        p_oARecMaster.updateObject("nOBalance", 0.00);
        p_oARecMaster.updateObject("nABalance", 0.00);
        p_oARecMaster.updateObject("dCltSince", p_oApp.getServerDate());

        p_oARecMaster.insertRow();
        p_oARecMaster.moveToCurrentRow();
    }

    public String getSQ_Term() {
        String lsSQL = "";

        lsSQL = "SELECT"
                + "  sTermCode"
                + ", sDescript"
                + ", CASE WHEN cCoverage = 0 Then 'Straight' "
                + "  WHEN cCoverage = 1 Then 'Days' "
                + " WHEN cCoverage = 2 Then 'Month' "
                + " ELSE 'Other' "
                + " END xCoverage "
                + ", nTermValx"
                + " FROM Term a"
                + " WHERE cRecdStat = 1";

        return lsSQL;
    }

    public boolean searchTerm(int fnRow, String fsValue, boolean fbByCode) throws SQLException {

        String lsSQL = getSQ_Term();
        if (fbByCode) {
            lsSQL = MiscUtil.addCondition(lsSQL, "sTermCode = " + SQLUtil.toSQL(fsValue));
        } else {
            lsSQL = MiscUtil.addCondition(lsSQL, "sDescript LIKE " + SQLUtil.toSQL(fsValue + "%"));
        }

        JSONObject loJSON;

        if (p_bWithUI) {
            loJSON = showFXDialog.jsonSearch(
                    p_oApp,
                    lsSQL,
                    fsValue,
                    "Code»Descript»Coverage»Value",
                    "sTermCode»sDescript»xCoverage»nTermValx",
                    "sTermCode»sDescript",
                    fbByCode ? 0 : 1);

            if (loJSON != null) {
                p_oARecMaster.absolute(fnRow);
                p_oARecMaster.updateString("sTermIDxx", (String) loJSON.get("sTermCode"));
                p_oARecMaster.updateString(27, (String) loJSON.get("sDescript"));
                p_oARecMaster.updateRow();

                if (p_oListener != null) {
                    p_oListener.DetailRetreive(fnRow, 27, p_oARecMaster.getObject(27));
                }
                return true;
            } else {
                p_sMessage = "No record selected.";
                return false;
            }
        }

        return true;
    }

    public String getSQ_Client() {
        String lsSQL = "";

        lsSQL = "SELECT "
                + "  a.sClientID"
                + " , a.sCompnyNm"
                + " , a.sClientNm"
                + " , a.sLastName"
                + " , a.sFrstName"
                + " , a.sMiddName"
                + " , a.sSuffixNm"
                + " , CONCAT(b.sHouseNox, ' ', b.sAddressx, ', ', c.sTownName, ', ', d.sProvName) xAddressx"
                + " FROM Client_Master a "
                + " LEFT JOIN Client_Address b "
                + " ON a.sClientID = b.sClientID "
                + " AND b.nPriority = 1 "
                + " LEFT JOIN TownCity c "
                + " ON b.sTownIDxx = c.sTownIDxx "
                + " LEFT JOIN Province d "
                + " ON c.sProvIDxx = d.sProvIDxx ";

        return lsSQL;
    }

    public boolean searchClient(int fnRow, String fsValue, boolean fbByCode) throws SQLException {

        if (p_nEditMode != EditMode.ADDNEW) {
            p_sMessage = "This feature was only for new entries.";
            return false;
        }

        String lsSQL = getSQ_Client();
        if (fbByCode) {
            lsSQL = MiscUtil.addCondition(lsSQL, "a.sClientID = " + SQLUtil.toSQL(fsValue));
        } else {
            lsSQL = MiscUtil.addCondition(lsSQL, "a.sCompnyNm LIKE " + SQLUtil.toSQL(fsValue + "%"));
        }

        JSONObject loJSON;

        if (p_bWithUI) {
            loJSON = showFXDialog.jsonSearch(
                    p_oApp,
                    lsSQL,
                    fsValue,
                    "ID»Company Name»Client Name»Address",
                    "sClientID»sCompnyNm»sClientNm»xAddressx",
                    "sClientID»sCompnyNm",
                    fbByCode ? 0 : 1);

            if (loJSON != null) {

                //check if already exist in data client
                lsSQL = "";

                lsSQL = MiscUtil.addCondition(getSQ_Master(), "a.sClientID = " + SQLUtil.toSQL((String) loJSON.get("sClientID")));
                lsSQL += " LIMIT 1";

                ResultSet loRS = p_oApp.executeQuery(lsSQL);

                loRS.last();
                if (loRS.getRow() >= 1) {
                    p_sMessage = "Unable to Select Existing Client!!! Please Inform MIS Division.. ";
                    return false;
                }

                p_oARecMaster.absolute(fnRow);
                p_oMaster.updateString("sClientID", (String) loJSON.get("sClientID"));
                p_oMaster.updateRow();

                p_oARecMaster.beforeFirst();
                while (p_oARecMaster.next()) {
                    p_oARecMaster.updateString("sClientID", (String) loJSON.get("sClientID"));
                    p_oARecMaster.updateString("sCompnyNm", (String) loJSON.get("sCompnyNm"));
                    p_oARecMaster.updateString("sLastName", (String) loJSON.get("sLastName"));
                    p_oARecMaster.updateString("sFrstName", (String) loJSON.get("sFrstName"));
                    p_oARecMaster.updateString("sMiddName", (String) loJSON.get("sMiddName"));
                    p_oARecMaster.updateString("sSuffixNm", (String) loJSON.get("sSuffixNm"));
                    p_oARecMaster.updateString("xAddressx", (String) loJSON.get("xAddressx"));

                    p_oARecMaster.updateRow();
                }
                if (p_oListener != null) {
                    p_oARecMaster.absolute(fnRow);
                    p_oListener.DetailRetreive(fnRow, 21, p_oARecMaster.getObject(21));
                    p_oListener.DetailRetreive(fnRow, 22, p_oARecMaster.getObject(22));
                    p_oListener.DetailRetreive(fnRow, 23, p_oARecMaster.getObject(23));
                    p_oListener.DetailRetreive(fnRow, 24, p_oARecMaster.getObject(24));
                    p_oListener.DetailRetreive(fnRow, 25, p_oARecMaster.getObject(25));
                    p_oListener.DetailRetreive(fnRow, 26, p_oARecMaster.getObject(26));
                }
                return true;
            } else {
                ClientFX oClient = new ClientFX(); //initialize main class
                ClientFX.poGRider = this.p_oApp;
                ClientFX.pnClientTp = 0;

                try {
                    CommonUtils.showModal(oClient);
                } catch (Exception ex) {
                    Logger.getLogger(ARDelivery.class.getName()).log(Level.SEVERE, null, ex);
                }

                if (oClient.getClientID().equals("")) {
                    p_oARecMaster.absolute(fnRow);
                    p_oARecMaster.updateString("sClientID", "");
                    p_oARecMaster.updateString("sCompnyNm", "");
                    p_oARecMaster.updateString("sLastName", "");
                    p_oARecMaster.updateString("sFrstName", "");
                    p_oARecMaster.updateString("sMiddName", "");
                    p_oARecMaster.updateString("sSuffixNm", "");
                    p_oARecMaster.updateString("xAddressx", "");
                    p_oMaster.updateString("sClientID", "");
                    p_oARecMaster.updateRow();
                    return false;
                } else {

                    lsSQL = getSQ_Client();
                    lsSQL = MiscUtil.addCondition(lsSQL, "a.sClientID = " + SQLUtil.toSQL(oClient.getClientID()));

                    JSONObject loJSONNew;
                    if (p_bWithUI) {
                        loJSONNew = showFXDialog.jsonSearch(
                                p_oApp,
                                lsSQL,
                                fsValue,
                                "ID»Company Name»Client Name»Address",
                                "sClientID»sCompnyNm»sClientNm»xAddressx",
                                "sClientID»sCompnyNm",
                                fbByCode ? 0 : 1);

                        if (loJSONNew != null) {
                            p_oARecMaster.absolute(fnRow);
                            p_oMaster.updateString("sClientID", (String) loJSONNew.get("sClientID"));
                            p_oMaster.updateRow();
                            p_oARecMaster.updateString("sClientID", (String) loJSONNew.get("sClientID"));
                            p_oARecMaster.updateString("sCompnyNm", (String) loJSONNew.get("sCompnyNm"));
                            p_oARecMaster.updateString("sLastName", (String) loJSONNew.get("sLastName"));
                            p_oARecMaster.updateString("sFrstName", (String) loJSONNew.get("sFrstName"));
                            p_oARecMaster.updateString("sMiddName", (String) loJSONNew.get("sMiddName"));
                            p_oARecMaster.updateString("sSuffixNm", (String) loJSONNew.get("sSuffixNm"));
                            p_oARecMaster.updateString("xAddressx", (String) loJSONNew.get("xAddressx"));

                            p_oARecMaster.updateRow();

                            if (p_oListener != null) {
                                p_oListener.DetailRetreive(fnRow, 21, p_oARecMaster.getObject(21));
                                p_oListener.DetailRetreive(fnRow, 22, p_oARecMaster.getObject(22));
                                p_oListener.DetailRetreive(fnRow, 23, p_oARecMaster.getObject(23));
                                p_oListener.DetailRetreive(fnRow, 24, p_oARecMaster.getObject(24));
                                p_oListener.DetailRetreive(fnRow, 25, p_oARecMaster.getObject(25));
                                p_oListener.DetailRetreive(fnRow, 26, p_oARecMaster.getObject(26));
                            }
                            return true;
                        } else {
                            return false;
                        }
                    }
                    return true;
                }

            }

        }
        return true;
    }

    private boolean isEntryOK() throws SQLException {

        //validate master
        p_oMaster.first();
        if (p_oMaster.getString("sRiderIDx").isEmpty()) {
            p_sMessage = "Transaction no must not be empty.";
            return false;
        }
        if (p_oMaster.getString("sClientID").isEmpty()) {
            p_sMessage = "Client must not be empty.";
            return false;
        }

        if (p_oMaster.getString("sDescript").isEmpty()) {
            p_sMessage = "Description  must not be empty.";
            return false;
        }

        if (p_oMaster.getString("sBriefDsc").isEmpty()) {
            p_sMessage = "Brief Description  must not be empty.";
            return false;
        }

        if (((Date) p_oMaster.getObject("dSrvcChrg")).compareTo(p_oApp.getSysDate()) <= 0) {
            p_sMessage = "Date Effectivity must not be less than or equal to the up-to-date";
            return false;
        }

        //validate detail
        if (getItemCount() == 0) {
            p_sMessage = "No Detail detected.";
            return false;
        }

        p_oARecMaster.beforeFirst();
        while (p_oARecMaster.next()) {

            if (p_oARecMaster.getString("sClientID").isEmpty()) {
                p_sMessage = "Client must not be empty.";
                return false;
            }
            if (p_oARecMaster.getString("sBranchCd").isEmpty()) {
                p_sMessage = "Branch must not be empty.";
                return false;
            }
            if (p_oARecMaster.getString("sTelNoxxx").isEmpty()) {
                p_sMessage = "Telephone no must not be empty.";
                return false;
            }
            if (p_oARecMaster.getString("sCPerson1").isEmpty()) {
                p_sMessage = "Contact Person must not be empty.";
                return false;
            }
            if (p_oARecMaster.getString("sTermIDxx").isEmpty()) {
                p_sMessage = "Term must not be empty.";
                return false;
            }
            if (p_oARecMaster.getString("sCPerson1").isEmpty()) {
                p_sMessage = "Contact Person must not be empty.";
                return false;
            }
            if (Double.parseDouble(p_oARecMaster.getString("nCredLimt")) <= 0.00) {
                p_sMessage = p_oARecMaster.getString("nCredLimt") + " Credit limit must be greater than 0 Amount.";
                return false;
            }
        }
        p_sMessage = "";
        if (p_nEditMode == EditMode.UPDATE) {
            System.out.print(CommonUtils.xsDateShort((Date) getMaster("dSrvcChrg")));
            if (p_oOldDate.equals(CommonUtils.xsDateShort((Date) getMaster("dSrvcChrg")))) {
                p_sMessage = "FYI : Date Effectivity is not modified. Delivery Service is not updated !";
                p_bSkipMaster = true;
            }
        }
        //check if mamager level
        if (!(GetUserApproval())) {
            return false;
        }
        return true;
    }

    public boolean GetUserApproval() {
        if (!(p_oApp.getUserLevel() >= UserRight.MANAGER)) {
            JSONObject approvalObject = showFXDialog.getApproval(p_oApp);
            if (approvalObject != null) {
                Object userLevelObject = approvalObject.get("nUserLevl");
                if (userLevelObject != null) {
                    int userLevel;
                    try {
                        userLevel = Integer.parseInt(userLevelObject.toString());
                        if (userLevel >= UserRight.MANAGER) {
                        } else {
                            p_sMessage = "Your account is not authorized to use this feature.";
                        }
                    } catch (NumberFormatException e) {
                    }
                }
            }
            p_sMessage = "Your account is not authorized to use this feature.";
            return false;
        }
        return true;
    }

    public boolean SaveTransaction() throws SQLException {
        if (p_oApp == null) {
            p_sMessage = "Application driver is not set.";
            return false;
        }

        p_sMessage = "";

        if (p_nEditMode != EditMode.ADDNEW
                && p_nEditMode != EditMode.UPDATE) {
            p_sMessage = "Invalid edit mode detected.";
            return false;
        }

        if (!isEntryOK()) {
            return false;
        }

        int lnCtr;
        String lsSQL;

        if (p_nEditMode == EditMode.ADDNEW) {
            if (!p_bWithParent) {
                p_oApp.beginTrans();
            }
            //save Delivery Service
            //set transaction number on records
            String lsRiderID = MiscUtil.getNextCode(MASTER_TABLE, "sRiderIDx", false, p_oApp.getConnection(), "");
            p_oMaster.updateObject("sRiderIDx", lsRiderID);
            p_oMaster.updateObject("dModified", p_oApp.getServerDate());
            p_oMaster.updateObject("sModified", p_oApp.getUserID());
            p_oMaster.updateRow();

            lsSQL = MiscUtil.rowset2SQL(p_oMaster,
                    MASTER_TABLE, "");

            if (p_oApp.executeQuery(lsSQL, MASTER_TABLE, p_sBranchCd, "") <= 0) {
                if (!p_bWithParent) {
                    p_oApp.rollbackTrans();
                }
                p_sMessage = p_oApp.getMessage() + ";" + p_oApp.getErrMsg();
                return false;
            }

            //save Delivery_Service_Charge_History
            lsSQL = "INSERT INTO " + HISTORY_TABLE + " SET"
                    + "  sRiderIDx = " + SQLUtil.toSQL(lsRiderID)
                    + ", nSrvcChrg = " + SQLUtil.toSQL(getMaster("nSrvcChrg"))
                    + ", dSrvcChrg = " + SQLUtil.toSQL(getMaster("dSrvcChrg"))
                    + ", dTimeStmp = " + SQLUtil.toSQL(p_oApp.getServerDate());

            if (p_oApp.executeQuery(lsSQL, HISTORY_TABLE, p_sBranchCd, "") <= 0) {
                if (!p_bWithParent) {
                    p_oApp.rollbackTrans();
                }
                p_sMessage = p_oApp.getMessage() + ";" + p_oApp.getErrMsg();
                return false;
            }

            //save AR_Master
            p_oARecMaster.beforeFirst();
            while (p_oARecMaster.next()) {
                p_oARecMaster.updateObject("sClientID", p_oMaster.getString("sClientID"));
                p_oARecMaster.updateRow();

                lsSQL = MiscUtil.rowset2SQL(p_oARecMaster, ARMASTER_TABLE, "sBranchNm;sCompnyNm;sLastName;sFrstName;sMiddName;sSuffixNm;xAddressx;xTermName;sDescript;");

                if (p_oApp.executeQuery(lsSQL, ARMASTER_TABLE, p_sBranchCd, "") <= 0) {
                    if (!p_bWithParent) {
                        p_oApp.rollbackTrans();
                    }
                    p_sMessage = p_oApp.getMessage() + ";" + p_oApp.getErrMsg();
                    return false;
                }

            }

            if (!p_bWithParent) {
                p_oApp.commitTrans();
            }

            p_nEditMode = EditMode.UNKNOWN;
            return true;
        } else {
            if (!p_bWithParent) {
                p_oApp.beginTrans();
            }

            if (!p_bSkipMaster) {
                //set transaction number on records
                String lsRiderx = (String) getMaster("sRiderIDx");

                lsSQL = MiscUtil.rowset2SQL(p_oMaster,
                        MASTER_TABLE,
                        "",
                        "sRiderIDx = " + SQLUtil.toSQL(lsRiderx));

                if (!lsSQL.isEmpty()) {
                    if (p_oApp.executeQuery(lsSQL, MASTER_TABLE, p_sBranchCd, "") <= 0) {
                        if (!p_bWithParent) {
                            p_oApp.rollbackTrans();
                        }
                        p_sMessage = p_oApp.getMessage() + ";" + p_oApp.getErrMsg();
                        return false;
                    }
                }

                //insert a Delivery_Service_Charge_History
                lsSQL = "INSERT INTO " + HISTORY_TABLE + " SET"
                        + "  sRiderIDx = " + SQLUtil.toSQL(lsRiderx)
                        + ", nSrvcChrg = " + SQLUtil.toSQL(getMaster("nSrvcChrg"))
                        + ", dSrvcChrg = " + SQLUtil.toSQL(getMaster("dSrvcChrg"))
                        + ", dTimeStmp = " + SQLUtil.toSQL(p_oApp.getServerDate());

                if (p_oApp.executeQuery(lsSQL, HISTORY_TABLE, p_sBranchCd, "") <= 0) {
                    if (!p_bWithParent) {
                        p_oApp.rollbackTrans();
                    }
                    p_sMessage = p_oApp.getMessage() + ";" + p_oApp.getErrMsg();
                    return false;
                }
            }
            p_oARecMaster.beforeFirst();
            while (p_oARecMaster.next()) {
                lsSQL = MiscUtil.rowset2SQL(p_oARecMaster,
                        ARMASTER_TABLE,
                        "sBranchNm;sCompnyNm;sLastName;sFrstName;sMiddName;sSuffixNm;xAddressx;xTermName;sDescript;",
                        "sClientID = " + SQLUtil.toSQL(getMaster("sClientID")));

                if (!lsSQL.isEmpty()) {
                    if (p_oApp.executeQuery(lsSQL, ARMASTER_TABLE, p_sBranchCd, "") <= 0) {
                        if (!p_bWithParent) {
                            p_oApp.rollbackTrans();
                        }
                        p_sMessage = p_oApp.getMessage() + ";" + p_oApp.getErrMsg();
                        return false;
                    }
                }

            }

            if (!p_bWithParent) {
                p_oApp.commitTrans();
            }

            p_nEditMode = EditMode.UNKNOWN;
            return true;
        }
    }

    public boolean SearchTransaction(String fsValue, boolean fbByCode) throws SQLException {
        if (p_oApp == null) {
            p_sMessage = "Application driver is not set.";
            return false;
        }

        p_sMessage = "";

        String lsSQL = getSQ_Master();
        if (fbByCode) {
            lsSQL = MiscUtil.addCondition(lsSQL, "a.sRiderIDx = " + SQLUtil.toSQL(fsValue));
        } else {
            lsSQL = MiscUtil.addCondition(lsSQL, "a.sDescript LIKE " + SQLUtil.toSQL(fsValue + "%"));
        }

        if (p_bWithUI) {
            JSONObject loJSON = showFXDialog.jsonSearch(
                    p_oApp,
                    lsSQL,
                    fsValue,
                    "ID.»Brief Description»Description»Date Partner",
                    "sRiderIDx»sBriefDsc»sDescript»dPartnerx",
                    "a.sRiderIDx»a.sBriefDsc»a.sDescript»a.dPartnerx",
                    fbByCode ? 0 : 1);
            if (loJSON != null) {
                return OpenTransaction((String) loJSON.get("sRiderIDx"));
            } else {
                p_sMessage = "No transaction found for the givern criteria.";
                return false;
            }
        }

        if (fbByCode) {
            lsSQL = MiscUtil.addCondition(lsSQL, "a.sRiderIDx = " + SQLUtil.toSQL(fsValue));
        } else {
            lsSQL = MiscUtil.addCondition(lsSQL, "a.sDescript LIKE " + SQLUtil.toSQL(fsValue + "%"));
            lsSQL += " LIMIT 1";
        }

        ResultSet loRS = p_oApp.executeQuery(lsSQL);

        if (!loRS.next()) {
            MiscUtil.close(loRS);
            p_sMessage = "No transaction found for the givern criteria.";
            return false;
        }

        lsSQL = loRS.getString("sRiderIDx");
        MiscUtil.close(loRS);

        return OpenTransaction(lsSQL);
    }

    public boolean OpenTransaction(String fsValue) throws SQLException {

        if (p_oApp == null) {
            p_sMessage = "Application driver is not set.";
            return false;
        }

        p_sMessage = "";
        String lsSQL = getSQ_Master();
        String lsCondition = "";
        ResultSet loRS;
        RowSetFactory factory = RowSetProvider.newFactory();

        lsSQL = MiscUtil.addCondition(getSQ_Master(), "a.sRiderIDx = " + SQLUtil.toSQL(fsValue));
        System.out.println(lsSQL);
        loRS = p_oApp.executeQuery(lsSQL);
        p_oMaster = factory.createCachedRowSet();
        p_oMaster.populate(loRS);
        MiscUtil.close(loRS);

        p_oMaster.last();
        p_oOldDate = getMaster("dSrvcChrg").toString();

        lsSQL = MiscUtil.addCondition(getSQ_Detail(), "a.sClientID = " + SQLUtil.toSQL(p_oMaster.getString("sClientID")));
        System.out.println(lsSQL);
        loRS = p_oApp.executeQuery(lsSQL);
        p_oARecMaster = factory.createCachedRowSet();
        p_oARecMaster.populate(loRS);
        MiscUtil.close(loRS);

        p_nEditMode = EditMode.READY;
        return true;
    }

    public boolean UpdateTransaction() throws SQLException {
        if (p_nEditMode != EditMode.READY) {
            p_sMessage = "Invalid edit mode.";
            return false;
        }

        p_nEditMode = EditMode.UPDATE;
        return true;
    }

    public boolean ActivateRecord() throws SQLException {
        if (p_nEditMode != EditMode.READY) {
            p_sMessage = "Invalid edit mode.";
            return false;
        }

        p_oMaster.first();

        if ("1".equals(p_oMaster.getString("cRecdStat"))) {
            p_sMessage = "Record is active.";
            return false;
        }

        String lsSQL = "UPDATE " + MASTER_TABLE + " SET"
                + "  cRecdStat = '1'"
                + ", sModified = " + SQLUtil.toSQL(p_oApp.getUserID())
                + ", dModified = " + SQLUtil.toSQL(p_oApp.getServerDate())
                + " WHERE sRiderIDx = " + SQLUtil.toSQL(p_oMaster.getString("sRiderIDx"));

        if (!p_bWithParent) {
            p_oApp.beginTrans();
        }
        if (p_oApp.executeQuery(lsSQL, MASTER_TABLE, p_sBranchCd, "") <= 0) {
            if (!p_bWithParent) {
                p_oApp.rollbackTrans();
            }
            p_sMessage = p_oApp.getMessage() + ";" + p_oApp.getErrMsg();
            return false;
        }
        if (!p_bWithParent) {
            p_oApp.commitTrans();
        }

        if (p_oListener != null) {
            p_oListener.MasterRetreive(8, "1");
        }

        p_nEditMode = EditMode.UNKNOWN;
        return true;
    }

    public boolean DeactivateRecord() throws SQLException {
        if (p_nEditMode != EditMode.READY) {
            p_sMessage = "Invalid edit mode.";
            return false;
        }

        p_oMaster.first();

        if ("0".equals(p_oMaster.getString("cRecdStat"))) {
            p_sMessage = "Record is inactive.";
            return false;
        }

        String lsSQL = "UPDATE " + MASTER_TABLE + " SET"
                + "  cRecdStat = '0'"
                + ", sModified = " + SQLUtil.toSQL(p_oApp.getUserID())
                + ", dModified = " + SQLUtil.toSQL(p_oApp.getServerDate())
                + " WHERE sRiderIDx = " + SQLUtil.toSQL(p_oMaster.getString("sRiderIDx"));

        if (!p_bWithParent) {
            p_oApp.beginTrans();
        }
        if (p_oApp.executeQuery(lsSQL, MASTER_TABLE, p_sBranchCd, "") <= 0) {
            if (!p_bWithParent) {
                p_oApp.rollbackTrans();
            }
            p_sMessage = p_oApp.getMessage() + ";" + p_oApp.getErrMsg();
            return false;
        }
        if (!p_bWithParent) {
            p_oApp.commitTrans();
        }

        if (p_oListener != null) {
            p_oListener.MasterRetreive(8, "0");
        }

        p_nEditMode = EditMode.UNKNOWN;
        return true;
    }

    public boolean removeDetail(int fnRow) throws SQLException {
        if (p_nEditMode != EditMode.ADDNEW && p_nEditMode != EditMode.UPDATE) {
            p_sMessage = "This feature was only for new/update entries.";
            return false;

        }

        if (getItemCount() == 1) {
            p_sMessage = "Unable to Delete Last Row!";
            return false;
        }

        //check if already exist in old data on update
        if (p_nEditMode == EditMode.UPDATE) {
            String lsSQL = "";

            lsSQL = MiscUtil.addCondition(getSQ_Detail(), "a.sClientID = " + SQLUtil.toSQL(getARDetail(fnRow, "sClientID"))
                    + " AND a.sBranchCd = " + SQLUtil.toSQL(getARDetail(fnRow, "sBranchCd")));
            lsSQL += " LIMIT 1";

            ResultSet loRS = p_oApp.executeQuery(lsSQL);

            loRS.last();
            if (loRS.getRow() >= 1) {
                p_sMessage = "Unable to Delete Existing Branch " + getARDetail(fnRow, "sBranchNm") + " Please Inform MIS Division.. ";
                return false;
            }
        }

        p_oARecMaster.absolute(fnRow);
        p_oARecMaster.deleteRow();

        return true;
    }

    public boolean addDetail() throws SQLException {
        int lnRox = getItemCount();

        if (p_nEditMode != EditMode.ADDNEW && p_nEditMode != EditMode.UPDATE) {
            p_sMessage = "This feature was only for new/update entries.";
            return false;

        }

        if (getARDetail(lnRox, "sClientID").toString().isEmpty()
                || getARDetail(lnRox, "sBranchCd").toString().isEmpty()) {

            p_sMessage = "Record of Client or Branch for last row was not yet set ...";
            return false;
        }

        String sClientID = (String) getARDetail(1, "sClientID");
        String sCompnyNm = (String) getARDetail(1, "sCompnyNm");
        String sLastName = (String) getARDetail(1, "sLastName");
        String sFrstName = (String) getARDetail(1, "sFrstName");
        String sMiddName = (String) getARDetail(1, "sMiddName");
        String sSuffixNm = (String) getARDetail(1, "sSuffixNm");
        String xAddressx = (String) getARDetail(1, "xAddressx");

        p_oARecMaster.last();
        p_oARecMaster.moveToInsertRow();
        MiscUtil.initRowSet(p_oARecMaster);
        p_oARecMaster.updateObject("sClientID", sClientID);
        p_oARecMaster.updateInt("cRecdStat", 1);
        p_oARecMaster.updateInt("cHoldAcct", 0);
        p_oARecMaster.updateInt("cAutoHold", 0);
        p_oARecMaster.updateString("sCompnyNm", sCompnyNm);
        p_oARecMaster.updateString("sLastName", sLastName);
        p_oARecMaster.updateString("sFrstName", sFrstName);
        p_oARecMaster.updateString("sMiddName", sMiddName);
        p_oARecMaster.updateString("sSuffixNm", sSuffixNm);
        p_oARecMaster.updateString("xAddressx", xAddressx);
        p_oARecMaster.updateString("sBranchNm", "");
        p_oARecMaster.updateString("sBranchCd", "");
        p_oARecMaster.updateObject("nDisCount", 0.00);
        p_oARecMaster.updateObject("nCredLimt", 0.00);
        p_oARecMaster.updateObject("dBalForwd", p_oApp.getServerDate());
        p_oARecMaster.updateObject("nBalForwd", 0.00);
        p_oARecMaster.updateObject("nOBalance", 0.00);
        p_oARecMaster.updateObject("nABalance", 0.00);
        p_oARecMaster.updateObject("dCltSince", p_oApp.getServerDate());
        p_oARecMaster.insertRow();
        p_oARecMaster.moveToCurrentRow();

        System.out.println(getItemCount());

        int fnRow = getItemCount();
        if (searchBranch(fnRow, "%", false)) {
            p_oARecMaster.absolute(fnRow);
            String BranchCd = p_oARecMaster.getString("sBranchCd");

            p_oARecMaster.beforeFirst();
            while (p_oARecMaster.next()) {
                if (p_oARecMaster.getRow() != fnRow) {

                    if (BranchCd.equals(p_oARecMaster.getString("sBranchCd"))) {
                        p_sMessage = p_oARecMaster.getString("sBranchNm") + " is Already exist... Please Add Different Branch";
                        p_oARecMaster.absolute(fnRow);
                        p_oARecMaster.deleteRow();
                        return false;
                    }
                }

            }
        } else {
            p_oARecMaster.absolute(fnRow);
            p_oARecMaster.deleteRow();

            p_sMessage = "Please Select a Branch to Add Detail!!!";
            return false;
        }

        return true;
    }

}
