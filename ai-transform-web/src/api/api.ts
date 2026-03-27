import request from '../request/axios';

export interface record {
    id:number,
    project_name:string,
    original_language:string,
    translated_language:string,
    original_video_url:string,
    translated_video_url:string,
    expiration_at:number,
    create_at:number,
}

export interface proofreadRecord {
    id: number
    project_name: string
    original_language: string
    translated_language: string
    status: string
    original_video_url: string
    translated_srt_for_proofread_url: string
    create_at: number
}

export function getTranslateRecords() {
    let path = '/v1/records'
    return request.get(path)
}

// 获取待校对任务列表
export function getPendingProofreads() {
    let path = '/v1/proofread/pending'
    return request.get(path)
}

// 获取字幕内容
export function getProofreadSrt(id: number) {
    let path = `/v1/proofread/${id}/srt`
    return request.get(path)
}

// 提交校对后的字幕
export function submitProofread(id: number, proofreadSrt: string) {
    let path = `/v1/proofread/${id}/submit`
    return request.post(path, { proofread_srt: proofreadSrt })
}


export interface transInfo {
    id?:number,  // ← 新增，加 ? 表示可选
    project_name:string,
    original_language:string,
    translate_language:string,
    proofread_type?:string,
    file_url:string,
}

export function translate(params:transInfo) {
    let path = '/v1/translate'
    let formData = new FormData()
    if(params.id) {  // ← 新增
        formData.append("id", params.id.toString())
    }
    formData.append("project_name",params.project_name)
    formData.append("original_language",params.original_language)
    formData.append("translate_language",params.translate_language)
    if(params.proofread_type) {
        formData.append("proofread_type", params.proofread_type)
    }
    formData.append("file_url",params.file_url)
    return request.post(path,formData)
}

export function cosPresignedUrl(filename:string){
    let path = '/v1/cos/presigned/url'
    return request.get(path + "?filename=" + filename)
}
export function uploadCos(presignedUrl:string,fileContent: ArrayBuffer) {
    return request.put(presignedUrl, fileContent)
}

// 失败任务接口定义
export interface failedRecord {
    id: number
    project_name: string
    original_language: string
    translated_language: string
    original_video_url: string
    error_message: string
    failed_at: number
    retry_count: number
    create_at: number
}

// 获取失败任务列表
export function getFailedRecords() {
    let path = '/v1/deadletter/failed'
    return request.get(path)
}

// 重试失败任务
export function retryFailedRecord(id: number) {
    let path = `/v1/deadletter/${id}/retry`
    return request.post(path)
}