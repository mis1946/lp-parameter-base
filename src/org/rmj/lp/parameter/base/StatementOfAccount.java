package org.rmj.lp.parameter.base;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.RowSetFactory;
import javax.sql.rowset.RowSetProvider;
import net.sf.jasperreports.engine.JRException;
import net.sf.jasperreports.engine.JasperFillManager;
import net.sf.jasperreports.engine.JasperPrint;
import net.sf.jasperreports.engine.data.JsonDataSource;
import net.sf.jasperreports.view.JasperViewer;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.rmj.appdriver.GRider;
import org.rmj.appdriver.MiscUtil;
import org.rmj.appdriver.SQLUtil;
import org.rmj.appdriver.agentfx.CommonUtils;
import org.rmj.appdriver.agentfx.ShowMessageFX;
import org.rmj.appdriver.agentfx.ui.showFXDialog;
import org.rmj.appdriver.constants.EditMode;
import org.rmj.appdriver.constants.RecordStatus;
import org.rmj.appdriver.constants.TransactionStatus;
import org.rmj.appdriver.constants.UserRight;
import org.rmj.cas.client.application.ClientFX;

/**
 *
 * @author Maynard
 */
public class StatementOfAccount {

    private final String MASTER_TABLE = "Billing_Master";
    private final String DETAIL_TABLE = "Billing_Detail";
    private final String DSDETAIL_TABLE = "Delivery_Service_Trans";
    private final String CIDETAIL_TABLE = "Charge_Invoice";

    private final GRider p_oApp;
    private final boolean p_bWithParent;
    private int p_nEditMode;
    private int p_nTranStat;

    private int p_nEntryCount = 0;

    private String p_sMessage;
    private String p_sBranchCd;
    private String p_sSourceCd;
    private String p_sDetBranch;

    private boolean p_bWithUI = true;
    private boolean p_bModified = false;
    private CachedRowSet p_oMaster;
    private CachedRowSet p_oDetail;

    private String p_oOldDate;

    private LMasDetTrans p_oListener;

    public StatementOfAccount(GRider foApp, String fsBranchCd, boolean fbWithParent) {
        p_oApp = foApp;
        p_sBranchCd = fsBranchCd;
        p_bWithParent = fbWithParent;

        if (p_sBranchCd.isEmpty()) {
            p_sBranchCd = p_oApp.getBranchCode();
        }

        p_nEditMode = EditMode.UNKNOWN;
    }

    public void setSource(String fsValue) {
        p_sSourceCd = fsValue;
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

    public String getSourceCd() {
        return p_sSourceCd;
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

    public int getBillItemCount() throws SQLException {
        if (p_oDetail == null) {
            return 0;
        }
        p_oDetail.last();
        return p_oDetail.getRow();
    }

    public Object getBillDetail(int fnRow, int fnIndex) throws SQLException {
        if (fnIndex == 0) {
            return null;
        }
        if (getBillItemCount() == 0 || fnRow > getBillItemCount()) {
            return null;
        }
        p_oDetail.absolute(fnRow);
        return p_oDetail.getObject(fnIndex);
    }

    public Object getBillDetail(int fnRow, String fsIndex) throws SQLException {
        return getBillDetail(fnRow, getColumnIndex(p_oDetail, fsIndex));
    }

    public void setBillDetail(int fnRow, int fnIndex, Object foValue) throws SQLException {
        if (getBillItemCount() == 0 || fnRow > getBillItemCount()) {
            return;
        }
        p_oDetail.absolute(fnRow);
        switch (fnIndex) {

            case 6://cBilledxx
                if (foValue instanceof Integer) {
                    p_oDetail.updateObject(fnIndex, foValue);
                    if (p_oDetail.getObject("cBilledxx").equals(1)) {
                        p_nEntryCount += 1;
                        p_bModified = true;
                    } else {
                        p_nEntryCount -= 1;
                        p_bModified = true;

                    }
                    System.out.println("item count" + p_nEntryCount);
                }
                break;

            case 11://cPaidxxxx
                if (foValue instanceof Integer) {
                    int cCollectd = p_oDetail.getInt("cCollectd");
                    if (cCollectd == 1) {

                        p_oDetail.updateObject(fnIndex, 1);
                        ShowMessageFX.Information("Unable to untag Paid Detail.  ", "Statement of Account -Tagging", "");
                        break;
                    }
                    if (p_oDetail.getObject("cPaidxxxx").equals(1)) {

                        p_oDetail.updateObject(fnIndex, foValue);
                        p_nEntryCount += 1;
                        p_bModified = true;
                    } else {

                        p_oDetail.updateObject(fnIndex, foValue);
                        p_nEntryCount -= 1;
                        p_bModified = true;

                    }
                    System.out.println("item count" + p_nEntryCount);
                }

                break;

        }
        p_oDetail.updateRow();
        if (p_oListener != null) {
            p_oListener.DetailRetreive(fnRow, fnIndex, p_oDetail.getObject(fnIndex));
        }
    }

    public void setBillDetail(int fnRow, String fsIndex, Object foValue) throws SQLException {
        setBillDetail(fnRow, getColumnIndex(p_oDetail, fsIndex), foValue);
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
            case 4://sSourceCd
            case 8://sRemarksx
                p_oMaster.updateString(fnIndex, ((String) foValue).trim());
                break;

            case 6://nAmtPaidx 
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
                + " a.sTransNox sTransNox "
                + " , a.dTransact dTransact "
                + " , a.sClientID sClientID "
                + " , a.sSourceCd sSourceCd "
                + " , a.nEntryNox nEntryNox "
                + " , a.nAmtPaidx nAmtPaidx "
                + " , a.nTranTotl nTranTotl "
                + " , a.sRemarksx sRemarksx "
                + " , a.cPrintedx cPrintedx "
                + " , a.dPrintedx dPrintedx "
                + " , a.cTranStat cTranStat "
                + " , a.sModified sModified "
                + " , a.dModified dModified "
                + " , a.dTimeStmp dTimeStmp "
                + " , b.sCompnyNm sCompnyNm "
                + " , b.sClientNm sClientNm "
                + " , CONCAT(c.sHouseNox, ' ', c.sAddressx, ', ', d.sTownName, ', ', e.sProvName) xAddressx"
                + " FROM " + MASTER_TABLE + " a "
                + " LEFT JOIN Client_Master b "
                + " ON a.sClientID = b.sClientID "
                + " LEFT JOIN Client_Address c "
                + " ON b.sClientID = c.sClientID "
                + " LEFT JOIN TownCity d "
                + " ON c.sTownIDxx = d.sTownIDxx "
                + " LEFT JOIN province e "
                + " ON d.sProvIDxx = e.sProvIDxx ";

        return lsSQL;

    }

    public String getSQ_Browse() {
        String lsSQL = "";

        String lsCondition = "";
        String lsStat = String.valueOf(p_nTranStat);

        if (lsStat.length() > 1) {
            for (int lnCtr = 0; lnCtr <= lsStat.length() - 1; lnCtr++) {
                lsSQL += ", " + SQLUtil.toSQL(Character.toString(lsStat.charAt(lnCtr)));
            }
            lsCondition = "a.cTranStat IN (" + lsSQL.substring(2) + ")";
        } else {
            lsCondition = "a.cTranStat = " + SQLUtil.toSQL(lsStat);
        }

        lsSQL = "SELECT "
                + " a.sTransNox sTransNox "
                + " , IFNULL(b.sCompnyNm, b.sClientNm) sCompnyNm "
                + " , a.dTransact dTransact "
                + " , a.sSourceCd sSourceCd "
                + " , CASE  "
                + " WHEN a.cTranStat = '0' THEN 'OPEN' "
                + " WHEN a.cTranStat = '1' THEN 'APPROVED' "
                + " WHEN a.cTranStat = '2' THEN 'FULLY PAID' "
                + " WHEN a.cTranStat = '3' THEN 'DISAPPROVED' "
                + " End As cTranStat "
                + " FROM " + MASTER_TABLE + " a "
                + " , Client_Master b "
                + " WHERE a.sClientID = b.sClientID AND " + lsCondition;

        return lsSQL;

    }

    public String getSQ_DeliveryService() {
        String lsSQL;

        lsSQL = "SELECT a.sTransNox "
                + " , b.sDescript "
                + " , a.sSourceNo "
                + " , c.dTransact "
                + " , a.nAmountxx "
                + " , a.cBilledxx "
                + " , a.sRemarksx "
                + " , a.sSourceCd "
                + " , a.cCollectd "
                + " , a.dBilledxx "
                + " , d.cPaidxxxx "
                + " , a.dPaidxxxx "
                + " , a.cWaivexxx "
                + " , a.dWaivexxx "
                + " , a.cTranStat "
                + " , b.sBriefDsc "
                + " , a.sRiderIDx "
                + " , b.sClientID "
                + " , e.sBranchCd "
                + " , e.sBranchNm "
                + " FROM " + DSDETAIL_TABLE + " a "
                + " LEFT JOIN Delivery_Service b "
                + " ON a.sRiderIDx = b.sRiderIDx "
                + " LEFT JOIN SO_Master c "
                + " ON a.sSourceCd = 'SO'"
                + " AND  a.sSourceNo = c.sTransNox "
                + " LEFT JOIN " + DETAIL_TABLE + " d "
                + " ON a.sTransNox = d.sSourceNo "
                + " LEFT JOIN Branch e "
                + " ON LEFT(a.sSourceNo,4) = e.sBranchCd ";

        return lsSQL;

    }

    public String getSQ_ChargeInvoice() {
        String lsSQL;

        lsSQL = "SELECT"
                + "  a.sTransNox"
                + " , IF(IFNULL(a.sClientID, '') = '', a.sClientNm, b.sCompnyNm) sClientNm"
                + " , a.sSourceNo"
                + " , c.dTransact"
                + " , a.nAmountxx"
                + " , a.cBilledxx"
                + " , a.sChargeNo"
                + " , a.sSourceCd"
                + " , a.cCollectd"
                + " , a.dBilledxx"
                + " , d.cPaidxxxx"
                + " , a.dPaidxxxx"
                + " , a.cWaivexxx"
                + " , a.dWaivexxx"
                + " , a.cTranStat"
                + " , a.sWaivexxx"
                + " , a.nDiscount"
                + " , a.nVatDiscx"
                + " , a.nPWDDiscx"
                + " , a.nAmtPaidx"
                + " , a.cTranStat"
                + " , IF(IFNULL(a.sClientID, '') = '', a.sClientNm, b.sCompnyNm) xxClientNm"
                + " , a.sClientID"
                + " , IF(IFNULL(e.sAddressx, '') = '', a.sAddressx, e.sAddressx) sAddressx"
                + " , a.sModified"
                + " , a.dModified"
                + " , c.dTransact"
                + " , f.sBranchCd"
                + " , f.sBranchNm"
                + " FROM Charge_Invoice a"
                + " LEFT JOIN Client_Master b"
                + " ON a.sClientID = b.sClientID"
                + " LEFT JOIN Client_Address e "
                + " ON b.sClientID = e.sClientID "
                + " LEFT JOIN SO_Master c"
                + " ON a.sSourceCd = 'SO'"
                + " AND a.sSourceNo = c.sTransNox"
                + " LEFT JOIN " + DETAIL_TABLE + " d "
                + " ON a.sTransNox = d.sSourceNo "
                + " LEFT JOIN Branch f "
                + " ON LEFT(a.sSourceNo,4) = f.sBranchCd ";

        return lsSQL;

    }

    public String getSQ_DService() {
        String lsSQL;

        lsSQL = "SELECT a.sRiderIDx"
                + " , a.sBriefDsc"
                + " , a.sDescript"
                + " , a.dPartnerx"
                + " FROM Delivery_Service a "
                + " WHERE a.cRecdStat = " + RecordStatus.ACTIVE
                + " ORDER BY a.sRiderIDx ";

        return lsSQL;

    }

    public boolean NewTransaction() throws SQLException {
        if (p_oApp == null) {
            p_sMessage = "Application driver is not set.";
            return false;
        }
        p_nEditMode = EditMode.UNKNOWN;
        p_sDetBranch = "";
        p_sMessage = "";

        p_sSourceCd = "";
        String lsSQL;
        ResultSet loRS;
        RowSetFactory factory = RowSetProvider.newFactory();

        //createcacherowset for master
        lsSQL = MiscUtil.addCondition(getSQ_Master(), "0=1");
        loRS = p_oApp.executeQuery(lsSQL);
        p_oMaster = factory.createCachedRowSet();
        p_oMaster.populate(loRS);
        MiscUtil.close(loRS);
        p_oDetail = null;
        //setobject required
        initMaster();

        p_nEditMode = EditMode.ADDNEW;
        return true;
    }

    private void initMaster() {
        try {
            p_oMaster.last();
            p_oMaster.moveToInsertRow();

            MiscUtil.initRowSet(p_oMaster);
            p_oMaster.updateObject("sTransNox", MiscUtil.getNextCode(MASTER_TABLE, "sTransNox", true, p_oApp.getConnection(), p_oApp.getBranchCode()));
            p_oMaster.updateObject("dTransact", p_oApp.getServerDate());
            p_oMaster.updateObject("dModified", p_oApp.getServerDate());
            p_oMaster.updateObject("dTimeStmp", p_oApp.getServerDate());
            p_oMaster.updateObject("sModified", p_oApp.getUserID());
            p_oMaster.updateObject("cPrintedx", RecordStatus.INACTIVE);

            p_oMaster.updateObject("cTranStat", RecordStatus.INACTIVE);
//            p_oMaster.updateObject("nTranTotl", 0.00);
            p_oMaster.insertRow();
            p_oMaster.moveToCurrentRow();

        } catch (SQLException ex) {
            Logger.getLogger(ARDelivery.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public String getSQ_ClientDS() {
        String lsSQL = "";

        lsSQL = "SELECT "
                + "  a.sClientID"
                + " , e.sBriefDsc"
                + " , a.sCompnyNm"
                + " , a.sClientNm"
                + " , CONCAT(b.sHouseNox, ' ', b.sAddressx, ', ', c.sTownName, ', ', d.sProvName) xAddressx"
                + " FROM Client_Master a "
                + " LEFT JOIN Client_Address b "
                + " ON a.sClientID = b.sClientID "
                + " AND b.nPriority = 1 "
                + " LEFT JOIN TownCity c "
                + " ON b.sTownIDxx = c.sTownIDxx "
                + " LEFT JOIN Province d "
                + " ON c.sProvIDxx = d.sProvIDxx "
                + " LEFT JOIN Delivery_Service e "
                + " ON a.sClientID = e.sClientID ";

        return lsSQL;
    }

    public boolean searchClient(String fsValue, boolean fbByCode) throws SQLException {
        switch (p_sSourceCd) {

            case "DS":
                if (!searchClientDS(fsValue, fbByCode)) {
                    return false;
                }
                break;
            case "CI":
                if (!searchClientCI(fsValue, fbByCode)) {
                    return false;
                }
                break;

        }
        return true;
    }

    private boolean searchClientDS(String fsValue, boolean fbByCode) throws SQLException {

        if (p_nEditMode != EditMode.ADDNEW) {
            p_sMessage = "Invalid Edit Mode";
            return false;
        }

        String lsSQL = getSQ_ClientDS();
        if (fbByCode) {
            lsSQL = MiscUtil.addCondition(lsSQL, "a.sClientNm LIKE " + SQLUtil.toSQL(fsValue + "%"));
        } else {
            lsSQL = MiscUtil.addCondition(lsSQL, "a.sCompnyNm LIKE " + SQLUtil.toSQL(fsValue + "%"));
        }

        JSONObject loJSON;

        System.out.println("ds query = " + lsSQL);
        if (p_bWithUI) {
            loJSON = showFXDialog.jsonSearch(
                    p_oApp,
                    lsSQL,
                    fsValue,
                    "Company Name»Client Name»Address»ID",
                    "sCompnyNm»sClientNm»xAddressx»sClientID",
                    "a.sCompnyNm»a.sClientNm»a.sClientID",
                    fbByCode ? 1 : 0);

            if (loJSON != null) {

                p_oMaster.last();
                p_oMaster.updateString("sClientID", (String) loJSON.get("sClientID"));
                p_oMaster.updateString("sCompnyNm", (String) loJSON.get("sCompnyNm"));
                p_oMaster.updateString("sClientNm", (String) loJSON.get("sClientNm"));
                p_oMaster.updateString("xAddressx", (String) loJSON.get("xAddressx"));
                p_oMaster.updateRow();

                if (p_oListener != null) {
                    p_oListener.MasterRetreive(15, p_oMaster.getObject(15));
                    p_oListener.MasterRetreive(16, p_oMaster.getObject(16));
                    p_oListener.MasterRetreive(17, p_oMaster.getObject(17));
                }
                if (!p_sDetBranch.isEmpty()) {
                    if (!loadBilling((String) loJSON.get("sClientID"), p_sDetBranch)) {
                        return false;
                    }
                }
                return true;
            } else {
                p_sMessage = "No record Found.";
                return false;
            }

        }
        return true;
    }

    public String getSQ_ClientCI() {
        String lsSQL = "";

        lsSQL = "SELECT "
                + "  a.sClientID"
                + " , a.sCompnyNm"
                + " , a.sClientNm"
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

    private boolean searchClientCI(String fsValue, boolean fbByCode) throws SQLException {

        if (p_nEditMode != EditMode.ADDNEW) {
            p_sMessage = "Invalid Edit Mode";
            return false;
        }

        String lsSQL = getSQ_ClientCI();
        if (fbByCode) {
            lsSQL = MiscUtil.addCondition(lsSQL, "a.sClientNm LIKE " + SQLUtil.toSQL(fsValue + "%"));
        } else {
            lsSQL = MiscUtil.addCondition(lsSQL, "a.sCompnyNm LIKE " + SQLUtil.toSQL(fsValue + "%"));
        }

        JSONObject loJSON;

        if (p_bWithUI) {
            loJSON = showFXDialog.jsonSearch(
                    p_oApp,
                    lsSQL,
                    fsValue,
                    "Company Name»Client Name»Address»ID",
                    "sCompnyNm»sClientNm»xAddressx»sClientID",
                    "a.sCompnyNm»a.sClientNm»a.sClientID",
                    fbByCode ? 1 : 0);

            if (loJSON != null) {

                p_oMaster.last();
                p_oMaster.updateString("sClientID", (String) loJSON.get("sClientID"));
                p_oMaster.updateString("sCompnyNm", (String) loJSON.get("sCompnyNm"));
                p_oMaster.updateString("sClientNm", (String) loJSON.get("sClientNm"));
                p_oMaster.updateString("xAddressx", (String) loJSON.get("xAddressx"));
                p_oMaster.updateRow();

                if (p_oListener != null) {
                    p_oListener.MasterRetreive(15, p_oMaster.getObject(15));
                    p_oListener.MasterRetreive(16, p_oMaster.getObject(16));
                    p_oListener.MasterRetreive(17, p_oMaster.getObject(17));
                }
                if (!loadBilling("", p_sDetBranch)) {
                    return false;
                }
                return true;
            } else {
                p_sMessage = "No record selected.";
                return false;
            }

        }

        return true;
    }

    public boolean loadBilling(String fsClientID, String lsBranch) {
        String lsSQL = "";

        if (p_sSourceCd == "DS") {
            if (fsClientID == "") {
                return false;

            }
            if (lsBranch == "") {
                return false;
            }
        }
        if (!p_sSourceCd.isEmpty()) {
            try {
                switch (p_sSourceCd) {

                    case "DS":
                        if (fsClientID != null && !fsClientID.isEmpty()) {
                            lsSQL = MiscUtil.addCondition(getSQ_DeliveryService(), "a.cBilledxx <> "
                                    + RecordStatus.ACTIVE
                                    + " AND b.sClientID = "
                                    + SQLUtil.toSQL(fsClientID)
                                    + " GROUP BY a.sSourceNo ");
                        } else {
                            lsSQL = MiscUtil.addCondition(getSQ_DeliveryService(), "a.cBilledxx <> "
                                    + RecordStatus.ACTIVE
                                    + " GROUP BY a.sSourceNo ");
                        }

                        break;
                    case "CI":
//                        if (fsClientID != null && !fsClientID.isEmpty()) {
//                            lsSQL = MiscUtil.addCondition(getSQ_ChargeInvoice(), "a.cBilledxx <> " 
//                                    + RecordStatus.ACTIVE
//                                    + " AND a.sClientID = " 
//                                    + SQLUtil.toSQL(fsClientID) 
//                                    + " GROUP BY a.sSourceNo ");
//                        } else {
                        lsSQL = MiscUtil.addCondition(getSQ_ChargeInvoice(), "a.cBilledxx <> " + RecordStatus.ACTIVE
                                + " GROUP BY a.sSourceNo ");
//                        }
                        break;

                }

                if (!lsBranch.isEmpty()) {
                    lsSQL = MiscUtil.addCondition(lsSQL, " sBranchCd = " + SQLUtil.toSQL(lsBranch));
                }

                if (!lsSQL.isEmpty()) {
                    ResultSet loRS;
                    RowSetFactory factory = RowSetProvider.newFactory();
                    System.out.println("Load Billing Query =  " + lsSQL);
                    loRS = p_oApp.executeQuery(lsSQL);
                    p_oDetail = factory.createCachedRowSet();
                    p_oDetail.populate(loRS);

                    MiscUtil.close(loRS);
                } else {
                    p_oDetail = null;
                    p_sMessage = "Its seems there is no Transaction to Bill ! Please double-check your entry...";
                    return false;
                }

                if (getBillItemCount() <= 0) {
                    p_oDetail = null;
                    p_sMessage = "Its seems there is no Transaction to Bill ! Please double-check your entry...";
                    return false;
                }
            } catch (SQLException ex) {
                Logger.getLogger(StatementOfAccount.class.getName()).log(Level.SEVERE, null, ex);
            }

        }

        return true;
    }

    public String BillngStatus() {
        try {
            p_oMaster.last();
            int cStatus = p_oMaster.getInt("cTranStat");
            switch (cStatus) {
                case 0:
                    return "OPEN";
                case 1:
                    return "APPROVED";
                case 2:
                    return "FULLY PAID";
                case 3:
                    return "DISAPPROVED";
                case 4:
                    return "WAIVED";
                default:
                    return "UNKNOWN";
            }
        } catch (SQLException ex) {
            Logger.getLogger(StatementOfAccount.class.getName()).log(Level.SEVERE, null, ex);
        }

        return "UNKNOWN";
    }

    private boolean isEntryOK() throws SQLException {

        //validate master
        p_oMaster.first();
        if (p_oMaster.getString("sSourceCd").isEmpty()) {
            p_sMessage = "Source must not be empty.";
            return false;
        }
        if (p_oMaster.getString("sClientID").isEmpty()) {
            p_sMessage = "Client must not be empty.";
            return false;
        }
        if (!p_bModified) {
            if (p_nEntryCount <= 0) {
                System.out.println("Detail seems to be Empty! Please check your entry...");
                return false;
            }
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
        String lsSQL = "";
        int lnEntryNox = 0;
        BigDecimal lnTotal = BigDecimal.ZERO;

        if (p_nEditMode == EditMode.ADDNEW) {
            if (!p_bWithParent) {
                p_oApp.beginTrans();
            }
        }

        String lsTransNo = MiscUtil.getNextCode(MASTER_TABLE, "sTransNox", true, p_oApp.getConnection(), p_sBranchCd);
        p_oMaster.updateObject("sTransNox", lsTransNo);
        p_oMaster.updateObject("dModified", p_oApp.getSysDate());
        p_oMaster.updateRow();

        p_oDetail.beforeFirst();
        while (p_oDetail.next()) {
            int cBilledxx = p_oDetail.getInt("cBilledxx");
            if (cBilledxx != 0) {

                BigDecimal nAmountxx = p_oDetail.getBigDecimal("nAmountxx");
                lnTotal = lnTotal.add(nAmountxx);
                lnEntryNox = lnEntryNox + 1;
                lsSQL = "INSERT INTO " + DETAIL_TABLE + " SET"
                        + "  sTransNox = " + SQLUtil.toSQL(p_oMaster.getString("sTransNox"))
                        + ", nEntryNox = " + lnEntryNox
                        + ", sSourceNo = " + SQLUtil.toSQL(p_oDetail.getObject("sTransNox"))
                        + ", nAmountxx = " + (p_oDetail.getBigDecimal("nAmountxx"))
                        + ", cPaidxxxx = " + RecordStatus.INACTIVE;

                if (p_oApp.executeQuery(lsSQL, DETAIL_TABLE, p_sBranchCd, "") <= 0) {
                    if (!p_bWithParent) {
                        p_oApp.rollbackTrans();
                    }
                    p_sMessage = p_oApp.getMessage() + ";" + p_oApp.getErrMsg();
                    return false;
                }

                switch (p_sSourceCd) {
                    case "DS":
                        lsSQL = "UPDATE " + DSDETAIL_TABLE + " SET"
                                + "  cBilledxx = " + RecordStatus.ACTIVE
                                + ", dBilledxx = " + SQLUtil.toSQL(p_oApp.getServerDate())
                                + " WHERE sTransNox = " + SQLUtil.toSQL(p_oDetail.getObject("sTransNox"));

                        if (p_oApp.executeQuery(lsSQL, DSDETAIL_TABLE, p_sBranchCd, "") <= 0) {
                            if (!p_bWithParent) {
                                p_oApp.rollbackTrans();
                            }
                            p_sMessage = p_oApp.getMessage() + ";" + p_oApp.getErrMsg();
                            return false;
                        }
                        break;
                    case "CI":
                        lsSQL = "UPDATE " + CIDETAIL_TABLE + " SET"
                                + "  cBilledxx = " + RecordStatus.ACTIVE
                                + ", dBilledxx = " + SQLUtil.toSQL(p_oApp.getServerDate())
                                + " WHERE sTransNox = " + SQLUtil.toSQL(p_oDetail.getObject("sTransNox"));

                        if (p_oApp.executeQuery(lsSQL, CIDETAIL_TABLE, p_sBranchCd, "") <= 0) {
                            if (!p_bWithParent) {
                                p_oApp.rollbackTrans();
                            }
                            p_sMessage = p_oApp.getMessage() + ";" + p_oApp.getErrMsg();
                            return false;
                        }
                        break;
                }
            }
        }
        if (p_sSourceCd.equals("DS")) {
            //update outstanding balance and account balance 
            String lsClientID = p_oMaster.getString("sClientID");
            lsSQL = "UPDATE AR_Master SET"
                    + "  nOBalance = nOBalance +" + lnTotal
                    + ", nABalance = nABalance -" + lnTotal
                    + " WHERE sBranchCd = " + SQLUtil.toSQL(p_sDetBranch)
                    + " AND sClientID = " + SQLUtil.toSQL(lsClientID);

            if (p_oApp.executeQuery(lsSQL, "AR_Master", p_sBranchCd, "") <= 0) {
                if (!p_bWithParent) {
                    p_oApp.rollbackTrans();
                }
                p_sMessage = p_oApp.getMessage() + ";" + p_oApp.getErrMsg();
                return false;
            }
        }

        p_oMaster.updateObject("nEntryNox", lnEntryNox);
        p_oMaster.updateBigDecimal("nTranTotl", lnTotal);
        p_oMaster.updateRow();
        System.out.println("Total Amount = " + p_oMaster.getBigDecimal("nTranTotl"));
        lsSQL = MiscUtil.rowset2SQL(p_oMaster,
                MASTER_TABLE, "sCompnyNm»sClientNm»xAddressx");

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

        p_nEditMode = EditMode.UNKNOWN;
        return true;

    }

    public String searchBranch(String fsValue, boolean fbByCode) throws SQLException {

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
                p_sDetBranch = (String) loJSON.get("sBranchCd");

                loadBilling(p_oMaster.getString("sClientID"), p_sDetBranch);

                return (String) loJSON.get("sBranchNm");

            } else {
                p_sDetBranch = "";
                p_sMessage = "No record selected.";
                return "";
            }
        }

        return "";
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

    public boolean SearchTransaction(String fsValue, boolean fbByCode) throws SQLException {
        if (p_oApp == null) {
            p_sMessage = "Application driver is not set.";
            return false;
        }

        p_sMessage = "";

        String lsSQL = getSQ_Browse();

        if (fbByCode) {
            lsSQL = MiscUtil.addCondition(lsSQL, "a.dTransact LIKE " + SQLUtil.toSQL(fsValue + "%"));
        } else {
            lsSQL = MiscUtil.addCondition(lsSQL, "a.sTransNox LIKE " + SQLUtil.toSQL(fsValue + "%"));
        }

        if (p_bWithUI) {
            JSONObject loJSON = showFXDialog.jsonSearch(
                    p_oApp,
                    lsSQL,
                    fsValue,
                    "Transaction No.»sCompnyNm»Date»sSourceCd»cTranStat",
                    "sTransNox»sCompnyNm»dTransact»sSourceCd»cTranStat",
                    "a.sTransNox»b.sCompnyNm»a.dTransact»a.cTranStat",
                    fbByCode ? 2 : 0);
            if (loJSON != null) {
                return OpenTransaction((String) loJSON.get("sTransNox"));
            } else {
                p_sMessage = "No transaction found for the givern criteria.";
                return false;
            }
        }

        if (fbByCode) {
            lsSQL = MiscUtil.addCondition(lsSQL, "a.dTransact = " + SQLUtil.toSQL(fsValue + "%"));
        } else {
            lsSQL = MiscUtil.addCondition(lsSQL, "a.sTransNox LIKE " + SQLUtil.toSQL(fsValue + "%"));
            lsSQL += " LIMIT 1";
        }

        ResultSet loRS = p_oApp.executeQuery(lsSQL);

        if (!loRS.next()) {
            MiscUtil.close(loRS);
            p_sMessage = "No transaction found for the givern criteria.";
            return false;
        }

        lsSQL = loRS.getString("sTransNox");
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
        ResultSet loRS;
        RowSetFactory factory = RowSetProvider.newFactory();

        lsSQL = MiscUtil.addCondition(getSQ_Master(), "a.sTransNox = " + SQLUtil.toSQL(fsValue));
        System.out.println(lsSQL);
        loRS = p_oApp.executeQuery(lsSQL);
        p_oMaster = factory.createCachedRowSet();
        p_oMaster.populate(loRS);
        MiscUtil.close(loRS);

        p_oMaster.last();
        p_sSourceCd = getMaster("sSourceCd").toString();

        if (p_sSourceCd.equals("DS")) {
            lsSQL = MiscUtil.addCondition(getSQ_DeliveryService(), "d.sTransNox = " + SQLUtil.toSQL(fsValue));

        } else {
            lsSQL = MiscUtil.addCondition(getSQ_ChargeInvoice(), "d.sTransNox = " + SQLUtil.toSQL(fsValue));

        }

        System.out.println(lsSQL);
        loRS = p_oApp.executeQuery(lsSQL);
        p_oDetail = factory.createCachedRowSet();
        p_oDetail.populate(loRS);
        MiscUtil.close(loRS);

        p_sDetBranch = (String) getBillDetail(1, "sBranchCd");

        if (getBillItemCount() <= 0) {
            p_nEditMode = EditMode.UNKNOWN;
            return false;
        }

        p_nEditMode = EditMode.READY;
        p_bModified = false;
        return true;

    }

    public boolean printBill() {
        try {
            if (p_oMaster.getString("cPrintedx").equals("1")) {
                if (!(GetUserApproval())) {
                    return false;
                }
            }

            String sTransNox = (String) getMaster("sTransNox");
            if (!OpenTransaction(sTransNox)) {
                return false;
            }
            JSONArray json_arr = new JSONArray();
            json_arr.clear();
            String lsSQL;
            String dTransact = CommonUtils.xsDateLong((Date) getMaster("dTransact"));
            BigDecimal nTranTotl = (BigDecimal) getMaster("nTranTotl");
            String sCompnyNm = (String) getMaster("sCompnyNm");
            String sClientNm = (String) getMaster("sClientNm");
            String xAddressx = (String) getMaster("xAddressx");
            int cTranStat = Integer.parseInt(getMaster("cTranStat").toString());
            BigDecimal nOBalance;
            BigDecimal nPrevTranTotl;
            String dPrintedx = CommonUtils.dateFormat(p_oApp.getServerDate(), "MMMM dd, yyyy -  hh:mm:ss");
            String nEntryNox = getMaster("nEntryNox").toString();

            String sClientID = (String) getMaster("sClientID");

            p_oDetail.beforeFirst();
            while (p_oDetail.next()) {
                JSONObject json_obj = new JSONObject();
                json_obj.put("sField01", p_oDetail.getString("sSourceNo"));
                json_obj.put("sField02", p_oDetail.getString("sBranchNm"));
                json_obj.put("sField03", CommonUtils.xsDateLong(p_oDetail.getDate("dTransact")));
                json_obj.put("nField01", p_oDetail.getBigDecimal("nAmountxx"));
                json_arr.add(json_obj);

                int cTranStatDet = p_oDetail.getInt("cTranStat");
                if (cTranStatDet != 2) {
                    if (p_sSourceCd.equals("DS")) {

                        lsSQL = "UPDATE Delivery_Service_Trans SET"
                                + " dBilledxx = " + SQLUtil.toSQL(p_oApp.getServerDate())
                                + ", cTranStat = " + RecordStatus.ACTIVE
                                + " WHERE sTransNox = " + SQLUtil.toSQL(p_oDetail.getString("sTransNox"));

                        if (p_oApp.executeQuery(lsSQL, "Delivery_Service_Trans", p_sBranchCd, "") <= 0) {
                            if (!p_bWithParent) {
                                p_oApp.rollbackTrans();
                            }
                            p_sMessage = p_oApp.getMessage() + ";" + p_oApp.getErrMsg();
                            return false;
                        }

                    } else {
                        lsSQL = "UPDATE Charge_Invoice SET"
                                + " dBilledxx = " + SQLUtil.toSQL(p_oApp.getServerDate())
                                + ", cTranStat = " + RecordStatus.ACTIVE
                                + " WHERE sTransNox = " + SQLUtil.toSQL(p_oDetail.getString("sTransNox"));

                        if (p_oApp.executeQuery(lsSQL, "Charge_Invoice", p_sBranchCd, "") <= 0) {
                            if (!p_bWithParent) {
                                p_oApp.rollbackTrans();
                            }
                            p_sMessage = p_oApp.getMessage() + ";" + p_oApp.getErrMsg();
                            return false;
                        }

                    }

                }
            }
            if (p_sSourceCd.equals("DS")) {
                lsSQL = "SELECT nOBalance FROM AR_Master WHERE sClientID = " + SQLUtil.toSQL(sClientID)
                        + " AND sBranchCd = " + SQLUtil.toSQL(p_sDetBranch);
                ResultSet loRS = p_oApp.executeQuery(lsSQL);

                if (loRS.next()) {
                    nOBalance = loRS.getBigDecimal("nOBalance");

                    nPrevTranTotl = nOBalance.subtract(nTranTotl);
                    loRS.close();
                } else {

                    loRS.close();
                    return false;
                }
            } else {
                nOBalance = BigDecimal.ZERO;
                nPrevTranTotl = BigDecimal.ZERO;
            }
            //Create the parameter
            Map<String, Object> params = new HashMap<>();
            params.put("sTransNox", sTransNox);
            params.put("dTransact", dTransact);
            params.put("nTranTotl", nTranTotl);
            params.put("sCompnyNm", sCompnyNm);
            params.put("sClientNm", sClientNm);
            params.put("xAddress", xAddressx);
            params.put("dPrintedx", dPrintedx);
            params.put("nOBalance", nOBalance);
            params.put("nPrevTranTotl", nPrevTranTotl);
            params.put("nEntryNox", nEntryNox);
            params.put("cTranStat", cTranStat);

//            lsSQL = "SELECT sClientNm FROM Client_Master WHERE sClientID IN ("
//                    + "SELECT sEmployNo FROM xxxSysUser WHERE sUserIDxx = " + SQLUtil.toSQL(p_oApp.getUserID()) + ")";
//            loRS = p_oApp.executeQuery(lsSQL);
//
//            if (loRS.next()) {
//                params.put("sPrepared", loRS.getString("sClientNm"));
//            } else {
//                params.put("sPrepared", "");
//            }
            lsSQL = "UPDATE " + MASTER_TABLE + " SET "
                    + "  cPrintedx = " + RecordStatus.ACTIVE
                    + ", dPrintedx = " + SQLUtil.toSQL(p_oApp.getServerDate())
                    + ", sModified = " + SQLUtil.toSQL(p_oApp.getUserID())
                    + ", dModified = " + SQLUtil.toSQL(p_oApp.getServerDate())
                    + " WHERE sTransNox = " + SQLUtil.toSQL(sTransNox);

            if (p_oApp.executeQuery(lsSQL, MASTER_TABLE, p_sBranchCd, "") <= 0) {
                if (!p_bWithParent) {
                    p_oApp.rollbackTrans();
                }
                p_sMessage = p_oApp.getMessage() + ";" + p_oApp.getErrMsg();
                return false;
            }

            InputStream stream = new ByteArrayInputStream(json_arr.toJSONString().getBytes("UTF-8"));
            JsonDataSource jrjson = new JsonDataSource(stream);

            JasperPrint _jrprint = JasperFillManager.fillReport("d:/GGC_Java_Systems/reports/BillingSOA.jasper", params, jrjson);
            JasperViewer jv = new JasperViewer(_jrprint, false);

            jv.setVisible(true);
            jv.setAlwaysOnTop(true);

            p_oMaster.setObject("cPrintedx", 1);
            return true;

        } catch (JRException | UnsupportedEncodingException | SQLException ex) {
            p_sMessage = ex.getMessage();
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

    public boolean CancelTransaction() throws SQLException {
        if (p_nEditMode != EditMode.READY) {
            p_sMessage = "Invalid update mode detected.";
            return false;
        }
        String lsSQL;

        p_sMessage = "";

        p_oDetail.beforeFirst();
        while (p_oDetail.next()) {
            if (p_sSourceCd.equals("DS")) {

                lsSQL = "UPDATE Delivery_Service_Trans SET"
                        + " cBilledxx = " + RecordStatus.INACTIVE
                        + ", cCollectd = " + RecordStatus.INACTIVE
                        + ", cTranStat = " + RecordStatus.INACTIVE
                        + ", dBilledxx = NULL "
                        + " WHERE sTransNox = " + SQLUtil.toSQL(p_oDetail.getString("sTransNox"));

                if (p_oApp.executeQuery(lsSQL, "Delivery_Service_Trans", p_sBranchCd, "") <= 0) {
                    if (!p_bWithParent) {
                        p_oApp.rollbackTrans();
                    }
                    p_sMessage = p_oApp.getMessage() + ";" + p_oApp.getErrMsg();
                    return false;
                }
            } else {
                lsSQL = "UPDATE Charge_Invoice SET"
                        + " cBilledxx = " + RecordStatus.INACTIVE
                        + ", cCollectd = " + RecordStatus.INACTIVE
                        + ", cTranStat = " + RecordStatus.INACTIVE
                        + ", dBilledxx = NULL "
                        + " WHERE sTransNox = " + SQLUtil.toSQL(p_oDetail.getString("sTransNox"));

                if (p_oApp.executeQuery(lsSQL, "Charge_Invoice", p_sBranchCd, "") <= 0) {
                    if (!p_bWithParent) {
                        p_oApp.rollbackTrans();
                    }
                    p_sMessage = p_oApp.getMessage() + ";" + p_oApp.getErrMsg();
                    return false;
                }

            }

        }
        BigDecimal lnTotal = p_oMaster.getBigDecimal("nTranTotl");
        //update outstanding balance and account balance 
        String lsClientID = p_oMaster.getString("sClientID");
        if (p_sSourceCd.equals("DS")) {
            lsSQL = "UPDATE AR_Master SET"
                    + "  nOBalance = nOBalance -" + lnTotal
                    + ", nABalance = nABalance +" + lnTotal
                    + " WHERE sBranchCd = " + SQLUtil.toSQL(p_sDetBranch)
                    + " AND sClientID = " + SQLUtil.toSQL(lsClientID);

            if (p_oApp.executeQuery(lsSQL, "AR_Master", p_sBranchCd, "") <= 0) {
                if (!p_bWithParent) {
                    p_oApp.rollbackTrans();
                }
                p_sMessage = p_oApp.getMessage() + ";" + p_oApp.getErrMsg();
                return false;
            }
        }

        String lsTransNox = (String) getMaster("sTransNox");
        lsSQL = "UPDATE " + MASTER_TABLE + " SET"
                + "  cTranStat = '3'"
                + ", sModified = " + SQLUtil.toSQL(p_oApp.getUserID())
                + ", dModified = " + SQLUtil.toSQL(p_oApp.getServerDate())
                + " WHERE sTransNox = " + SQLUtil.toSQL(lsTransNox);

        if (p_oApp.executeQuery(lsSQL, MASTER_TABLE, p_sBranchCd, "") <= 0) {
            p_sMessage = p_oApp.getErrMsg() + "; " + p_oApp.getMessage();
            return false;
        }

        p_nEditMode = EditMode.UNKNOWN;
        return true;
    }

    public boolean CloseTransaction() throws SQLException {
        if (p_nEditMode != EditMode.READY) {
            p_sMessage = "Invalid update mode detected.";
            return false;
        }
        String lsSQL;

        p_sMessage = "";

        String lsTransNox = (String) getMaster("sTransNox");
        lsSQL = "UPDATE " + MASTER_TABLE + " SET"
                + "  cTranStat = '1'"
                + ", sModified = " + SQLUtil.toSQL(p_oApp.getUserID())
                + ", dModified = " + SQLUtil.toSQL(p_oApp.getServerDate())
                + " WHERE sTransNox = " + SQLUtil.toSQL(lsTransNox);

        if (p_oApp.executeQuery(lsSQL, MASTER_TABLE, p_sBranchCd, "") <= 0) {
            p_sMessage = p_oApp.getErrMsg() + "; " + p_oApp.getMessage();
            return false;
        }

        p_nEditMode = EditMode.UNKNOWN;
        return true;
    }

    public boolean PrePostTransaction() throws SQLException {
        if (p_oApp == null) {
            p_sMessage = "Application driver is not set.";
            return false;
        }

        p_sMessage = "";

        if (p_nEditMode != EditMode.READY
                && p_nEditMode != EditMode.UPDATE) {
            p_sMessage = "Invalid edit mode detected.";
            return false;
        }

        if (!isEntryOK()) {
            return false;
        }
        String lsSQL = "";

        if (!p_bWithParent) {
            p_oApp.beginTrans();
        }

        p_oDetail.beforeFirst();
        while (p_oDetail.next()) {

            if (p_oDetail.getInt("cPaidxxxx") != 0) {
                lsSQL = "UPDATE " + DETAIL_TABLE + " SET "
                        + "  cPaidxxxx = " + RecordStatus.ACTIVE
                        + " WHERE sTransNox = " + SQLUtil.toSQL(p_oMaster.getString("sTransNox"))
                        + " AND sSourceNo = " + SQLUtil.toSQL(p_oDetail.getString("sTransNox"));

                if (p_oApp.executeQuery(lsSQL, DETAIL_TABLE, p_sBranchCd, "") <= 0) {
                    if (!p_bWithParent) {
                        p_oApp.rollbackTrans();
                    }
                    p_sMessage = p_oApp.getMessage() + ";" + p_oApp.getErrMsg();
                    return false;
                }

                switch (p_sSourceCd) {
                    case "DS":
                        lsSQL = "UPDATE " + DSDETAIL_TABLE + " SET"
                                + "  cCollectd = " + RecordStatus.ACTIVE
                                + ", cPaidxxxx = " + RecordStatus.ACTIVE
                                + ", dPaidxxxx = " + SQLUtil.toSQL(p_oApp.getServerDate())
                                + ", cTranStat = " + RecordStatus.ACTIVE
                                + ", dModified = " + SQLUtil.toSQL(p_oApp.getServerDate())
                                + " WHERE sTransNox = " + SQLUtil.toSQL(p_oDetail.getObject("sTransNox"));

                        if (p_oApp.executeQuery(lsSQL, DSDETAIL_TABLE, p_sBranchCd, "") <= 0) {
                            if (!p_bWithParent) {
                                p_oApp.rollbackTrans();
                            }
                            p_sMessage = p_oApp.getMessage() + ";" + p_oApp.getErrMsg();
                            return false;
                        }
                        break;
                    case "CI":
                        lsSQL = "UPDATE " + CIDETAIL_TABLE + " SET"
                                + "  cCollectd = " + RecordStatus.ACTIVE
                                + ", cPaidxxxx = " + RecordStatus.ACTIVE
                                + ", dPaidxxxx = " + SQLUtil.toSQL(p_oApp.getServerDate())
                                + ", cTranStat = " + RecordStatus.ACTIVE
                                + ", dModified = " + SQLUtil.toSQL(p_oApp.getServerDate())
                                + " WHERE sTransNox = " + SQLUtil.toSQL(p_oDetail.getObject("sTransNox"));

                        if (p_oApp.executeQuery(lsSQL, CIDETAIL_TABLE, p_sBranchCd, "") <= 0) {
                            if (!p_bWithParent) {
                                p_oApp.rollbackTrans();
                            }
                            p_sMessage = p_oApp.getMessage() + ";" + p_oApp.getErrMsg();
                            return false;
                        }
                        break;
                }
            }
        }

        String lsClientID = p_oMaster.getString("sClientID");
        BigDecimal nAmtPaidx = p_oMaster.getBigDecimal("nAmtPaidx");
        if (p_sSourceCd.equals("DS")) {

            int lnEntryNox;
            lsSQL = "SELECT nEntryNox FROM AR_Ledger "
                    + " WHERE sClientID = " + SQLUtil.toSQL(lsClientID)
                    + " AND sBranchCd = " + SQLUtil.toSQL(p_sDetBranch);
            ResultSet loRS = p_oApp.executeQuery(lsSQL);

            if (!loRS.next()) {
                lnEntryNox = 1;
            } else {
                lnEntryNox = loRS.getInt("nEntryNox") + 1;
            }

            loRS.close();
            //insert a ledger for amount paid
            lsSQL = "INSERT INTO AR_Ledger SET"
                    + "  sClientID = " + SQLUtil.toSQL(lsClientID)
                    + ", sBranchCd = " + SQLUtil.toSQL(p_sBranchCd)
                    + ", nEntryNox = " + lnEntryNox
                    + ", dTransact = " + SQLUtil.toSQL(p_oMaster.getString("dTransact"))
                    + ", sSourceCd = 'SA'"
                    + ", sSourceNo = " + SQLUtil.toSQL(p_oMaster.getString("sTransNox"))
                    + ", cReversex = '-' "
                    + ", nCreditxx = NULL"
                    + ", nDebitxxx = " + nAmtPaidx
                    + ", dPostedxx = " + SQLUtil.toSQL(p_oApp.getServerDate())
                    + " ON DUPLICATE KEY UPDATE "
                    + " nDebitxxx = " + (p_oMaster.getBigDecimal("nAmtPaidx"));

            if (p_oApp.executeQuery(lsSQL, "AR_Ledger", p_sBranchCd, "") <= 0) {
                if (!p_bWithParent) {
                    p_oApp.rollbackTrans();
                }
                p_sMessage = p_oApp.getMessage() + ";" + p_oApp.getErrMsg();
                return false;

            }
            //update ar master 
            lsSQL = "UPDATE AR_Master SET"
                    + "  nOBalance = nOBalance -" + (p_oMaster.getBigDecimal("nAmtPaidx"))
                    + " WHERE sBranchCd = " + SQLUtil.toSQL(p_sDetBranch)
                    + " AND sClientID = " + SQLUtil.toSQL(lsClientID);

            if (p_oApp.executeQuery(lsSQL, "AR_Master", p_sBranchCd, "") <= 0) {
                if (!p_bWithParent) {
                    p_oApp.rollbackTrans();
                }
                p_sMessage = p_oApp.getMessage() + ";" + p_oApp.getErrMsg();
                return false;
            }
        }

        String sTransNox = (String) getMaster("sTransNox");
        lsSQL = "UPDATE " + MASTER_TABLE + " SET "
                + " nAmtPaidx = " + nAmtPaidx
                + ", sModified = " + SQLUtil.toSQL(p_oApp.getUserID())
                + ", dModified = " + SQLUtil.toSQL(p_oApp.getServerDate())
                + " WHERE sTransNox = " + SQLUtil.toSQL(sTransNox);

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

        if (isAllPaid()) {
            if (!PostTransaction()) {

            }

        }
        p_nEditMode = EditMode.UNKNOWN;
        return true;

    }

    private boolean isAllPaid() {

        try {
            p_oDetail.beforeFirst();
            while (p_oDetail.next()) {
                int cPaidxxxx = p_oDetail.getInt("cPaidxxxx");
                if (cPaidxxxx != 1) {
                    return false;
                }
            }
        } catch (SQLException ex) {
            Logger.getLogger(StatementOfAccount.class.getName()).log(Level.SEVERE, null, ex);
        }
        return true;
    }

    public boolean PostTransaction() throws SQLException {
        if (p_oApp == null) {
            p_sMessage = "Application driver is not set.";
            return false;
        }

        p_sMessage = "";

        if (p_nEditMode != EditMode.READY
                && p_nEditMode != EditMode.UPDATE) {
            p_sMessage = "Invalid edit mode detected.";
            return false;
        }

        String lsSQL = "";

        if (!p_bWithParent) {
            p_oApp.beginTrans();
        }

        String sTransNox = (String) getMaster("sTransNox");
        lsSQL = "UPDATE " + MASTER_TABLE + " SET "
                + " cTranStat = " + TransactionStatus.STATE_POSTED
                + ", sModified = " + SQLUtil.toSQL(p_oApp.getUserID())
                + ", dModified = " + SQLUtil.toSQL(p_oApp.getServerDate())
                + " WHERE sTransNox = " + SQLUtil.toSQL(sTransNox);

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

        ShowMessageFX.Information("The Transaction is now FULLY PAID.  ", "Statement of Account -Tagging", "");

        return true;

    }
}
