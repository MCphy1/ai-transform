package data

import (
	"ai-transform-backend/pkg/constants"
	"database/sql"
	"fmt"
	"strings"
)

const TBL_TRANSFORM_RECORDS = "transform_records"

type TransformRecords struct {
	ID                           int64
	UserID                       int64
	ProjectName                  string
	OriginalLanguage             string
	TranslatedLanguage           string
	Status                       string
	ProofreadType                string
	OriginalVideoUrl             string
	OriginalSrtUrl               string
	TranslatedSrtUrl             string
	TranslatedSrtForProofreadUrl string
	ProofreadSrtUrl              string
	TranslatedVideoUrl           string
	ExpirationAt                 int64
	CreateAt                     int64
	UpdateAt                     int64
	ErrorMessage                 string
	FailedAt                     int64
	RetryCount                   int
}
type ITransformRecordsData interface {
	GetByID(id int64) (*TransformRecords, error)
	GetByUserID(userID int64) ([]*TransformRecords, error)
	GetPendingProofreadsByUserID(userID int64) ([]*TransformRecords, error)
	GetFailedByUserID(userID int64) ([]*TransformRecords, error)
	Add(entity *TransformRecords) error
	Update(entity *TransformRecords) error
	UpdateStatus(id int64, status string, translatedSrtForProofreadUrl string, updateAt int64) error
	UpdateProofreadResult(id int64, proofreadSrtUrl string, status string, updateAt int64) error
	UpdateFailedInfo(id int64, errorMessage string, failedAt int64) error
	ClearTranslationResult(id int64, proofreadType string, updateAt int64) error // 用于重试，清空结果
}

type transformRecordsData struct {
	table string
	db    *sql.DB
}

func (d *transformRecordsData) GetByID(id int64) (*TransformRecords, error) {
	sqlStr := fmt.Sprintf("select id,user_id,project_name,original_language,translated_language,status,proofread_type,original_video_url,original_srt_url,translated_srt_url,translated_srt_for_proofread_url,proofread_srt_url,translated_video_url,expiration_at,create_at,update_at,error_message,failed_at,retry_count from %s where id = ?", d.table)
	row := d.db.QueryRow(sqlStr, id)
	entity := &TransformRecords{}
	var nullErrorMessage sql.NullString
	var nullFailedAt sql.NullInt64
	err := row.Scan(&entity.ID, &entity.UserID, &entity.ProjectName, &entity.OriginalLanguage, &entity.TranslatedLanguage, &entity.Status, &entity.ProofreadType, &entity.OriginalVideoUrl, &entity.OriginalSrtUrl, &entity.TranslatedSrtUrl, &entity.TranslatedSrtForProofreadUrl, &entity.ProofreadSrtUrl, &entity.TranslatedVideoUrl, &entity.ExpirationAt, &entity.CreateAt, &entity.UpdateAt, &nullErrorMessage, &nullFailedAt, &entity.RetryCount)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if nullErrorMessage.Valid {
		entity.ErrorMessage = nullErrorMessage.String
	}
	if nullFailedAt.Valid {
		entity.FailedAt = nullFailedAt.Int64
	}
	return entity, nil
}

func (d *transformRecordsData) GetByUserID(userID int64) ([]*TransformRecords, error) {
	sqlStr := fmt.Sprintf("select id,user_id,project_name,original_language,translated_language,status,proofread_type,original_video_url,original_srt_url,translated_srt_url,translated_srt_for_proofread_url,proofread_srt_url,translated_video_url,expiration_at,create_at,update_at,error_message,failed_at,retry_count from %s where user_id = ? order by id desc", d.table)
	rows, err := d.db.Query(sqlStr, userID)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	defer rows.Close()
	list := make([]*TransformRecords, 0, 10)
	for rows.Next() {
		entity := &TransformRecords{}
		var nullErrorMessage sql.NullString
		var nullFailedAt sql.NullInt64
		err = rows.Scan(&entity.ID, &entity.UserID, &entity.ProjectName, &entity.OriginalLanguage, &entity.TranslatedLanguage, &entity.Status, &entity.ProofreadType, &entity.OriginalVideoUrl, &entity.OriginalSrtUrl, &entity.TranslatedSrtUrl, &entity.TranslatedSrtForProofreadUrl, &entity.ProofreadSrtUrl, &entity.TranslatedVideoUrl, &entity.ExpirationAt, &entity.CreateAt, &entity.UpdateAt, &nullErrorMessage, &nullFailedAt, &entity.RetryCount)
		if err != nil {
			return nil, err
		}
		if nullErrorMessage.Valid {
			entity.ErrorMessage = nullErrorMessage.String
		}
		if nullFailedAt.Valid {
			entity.FailedAt = nullFailedAt.Int64
		}
		list = append(list, entity)
	}
	return list, err
}

func (d *transformRecordsData) Add(entity *TransformRecords) error {
	sqlStr := fmt.Sprintf("insert into %s (user_id,project_name,original_language,translated_language,status,proofread_type,original_video_url,original_srt_url,translated_srt_url,translated_srt_for_proofread_url,proofread_srt_url,translated_video_url,expiration_at,create_at,update_at)values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", d.table)
	res, err := d.db.Exec(sqlStr, &entity.UserID, &entity.ProjectName, &entity.OriginalLanguage, &entity.TranslatedLanguage, &entity.Status, &entity.ProofreadType, &entity.OriginalVideoUrl, &entity.OriginalSrtUrl, &entity.TranslatedSrtUrl, &entity.TranslatedSrtForProofreadUrl, &entity.ProofreadSrtUrl, &entity.TranslatedVideoUrl, &entity.ExpirationAt, &entity.CreateAt, &entity.UpdateAt)
	if err != nil {
		return err
	}
	entity.ID, err = res.LastInsertId()
	if err != nil {
		return err
	}
	return nil
}

func (d *transformRecordsData) Update(entity *TransformRecords) error {
	updateFields := make([]string, 0, 10)
	params := make([]any, 0, 10)
	if entity.OriginalSrtUrl != "" {
		updateFields = append(updateFields, "original_srt_url = ?")
		params = append(params, entity.OriginalSrtUrl)
	}
	if entity.TranslatedSrtUrl != "" {
		updateFields = append(updateFields, "translated_srt_url = ?")
		params = append(params, entity.TranslatedSrtUrl)
	}
	if entity.TranslatedVideoUrl != "" {
		updateFields = append(updateFields, "translated_video_url = ?")
		params = append(params, entity.TranslatedVideoUrl)
	}
	if entity.Status != "" {
		updateFields = append(updateFields, "status = ?")
		params = append(params, entity.Status)
	}
	if entity.ExpirationAt != 0 {
		updateFields = append(updateFields, "expiration_at = ?")
		params = append(params, entity.ExpirationAt)
	}
	if len(params) == 0 {
		return nil
	}
	updateFields = append(updateFields, "update_at = ?")
	params = append(params, entity.UpdateAt)
	params = append(params, entity.ID)

	sqlStr := fmt.Sprintf("update %s set %s where id = ?", d.table, strings.Join(updateFields, ","))
	_, err := d.db.Exec(sqlStr, params...)
	if err != nil {
		return err
	}
	return nil
}

func (d *transformRecordsData) ClearTranslationResult(id int64, proofreadType string, updateAt int64) error {
	sqlStr := fmt.Sprintf("UPDATE %s SET original_srt_url = '', translated_srt_url = '', translated_video_url = '', expiration_at = 0, proofread_type = ?, update_at = ? WHERE id = ?", d.table)
	_, err := d.db.Exec(sqlStr, proofreadType, updateAt, id)
	return err
}

// GetPendingProofreadsByUserID 获取用户待校对的任务列表
func (d *transformRecordsData) GetPendingProofreadsByUserID(userID int64) ([]*TransformRecords, error) {
	sqlStr := fmt.Sprintf("select id,user_id,project_name,original_language,translated_language,status,proofread_type,original_video_url,original_srt_url,translated_srt_url,translated_srt_for_proofread_url,proofread_srt_url,translated_video_url,expiration_at,create_at,update_at,error_message,failed_at,retry_count from %s where user_id = ? and status = 'proofreading' order by id desc", d.table)
	rows, err := d.db.Query(sqlStr, userID)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	list := make([]*TransformRecords, 0, 10)
	for rows.Next() {
		entity := &TransformRecords{}
		var nullErrorMessage sql.NullString
		var nullFailedAt sql.NullInt64
		err = rows.Scan(&entity.ID, &entity.UserID, &entity.ProjectName, &entity.OriginalLanguage, &entity.TranslatedLanguage, &entity.Status, &entity.ProofreadType, &entity.OriginalVideoUrl, &entity.OriginalSrtUrl, &entity.TranslatedSrtUrl, &entity.TranslatedSrtForProofreadUrl, &entity.ProofreadSrtUrl, &entity.TranslatedVideoUrl, &entity.ExpirationAt, &entity.CreateAt, &entity.UpdateAt, &nullErrorMessage, &nullFailedAt, &entity.RetryCount)
		if err != nil {
			return nil, err
		}
		if nullErrorMessage.Valid {
			entity.ErrorMessage = nullErrorMessage.String
		}
		if nullFailedAt.Valid {
			entity.FailedAt = nullFailedAt.Int64
		}
		list = append(list, entity)
	}
	return list, nil
}

// UpdateStatus 更新任务状态和待校对字幕URL
func (d *transformRecordsData) UpdateStatus(id int64, status string, translatedSrtForProofreadUrl string, updateAt int64) error {
	sqlStr := fmt.Sprintf("UPDATE %s SET status = ?, translated_srt_for_proofread_url = ?, update_at = ? WHERE id = ?", d.table)
	_, err := d.db.Exec(sqlStr, status, translatedSrtForProofreadUrl, updateAt, id)
	return err
}

// UpdateProofreadResult 更新校对结果
func (d *transformRecordsData) UpdateProofreadResult(id int64, proofreadSrtUrl string, status string, updateAt int64) error {
	sqlStr := fmt.Sprintf("UPDATE %s SET proofread_srt_url = ?, status = ?, update_at = ? WHERE id = ?", d.table)
	_, err := d.db.Exec(sqlStr, proofreadSrtUrl, status, updateAt, id)
	return err
}

// GetFailedByUserID 获取用户失败的任务列表
func (d *transformRecordsData) GetFailedByUserID(userID int64) ([]*TransformRecords, error) {
	sqlStr := fmt.Sprintf("select id,user_id,project_name,original_language,translated_language,status,proofread_type,original_video_url,original_srt_url,translated_srt_url,translated_srt_for_proofread_url,proofread_srt_url,translated_video_url,expiration_at,create_at,update_at,error_message,failed_at,retry_count from %s where user_id = ? and status = ? order by id desc", d.table)
	rows, err := d.db.Query(sqlStr, userID, constants.STATUS_FAILED)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	list := make([]*TransformRecords, 0, 10)
	for rows.Next() {
		entity := &TransformRecords{}
		var nullErrorMessage sql.NullString
		var nullFailedAt sql.NullInt64
		err = rows.Scan(&entity.ID, &entity.UserID, &entity.ProjectName, &entity.OriginalLanguage, &entity.TranslatedLanguage, &entity.Status, &entity.ProofreadType, &entity.OriginalVideoUrl, &entity.OriginalSrtUrl, &entity.TranslatedSrtUrl, &entity.TranslatedSrtForProofreadUrl, &entity.ProofreadSrtUrl, &entity.TranslatedVideoUrl, &entity.ExpirationAt, &entity.CreateAt, &entity.UpdateAt, &nullErrorMessage, &nullFailedAt, &entity.RetryCount)
		if err != nil {
			return nil, err
		}
		if nullErrorMessage.Valid {
			entity.ErrorMessage = nullErrorMessage.String
		}
		if nullFailedAt.Valid {
			entity.FailedAt = nullFailedAt.Int64
		}
		list = append(list, entity)
	}
	return list, nil
}

// UpdateFailedInfo 更新失败信息
func (d *transformRecordsData) UpdateFailedInfo(id int64, errorMessage string, failedAt int64) error {
	sqlStr := fmt.Sprintf("UPDATE %s SET error_message = ?, failed_at = ?, retry_count = retry_count + 1, update_at = ? WHERE id = ?", d.table)
	_, err := d.db.Exec(sqlStr, errorMessage, failedAt, failedAt, id)
	return err
}
