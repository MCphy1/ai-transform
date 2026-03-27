<template>
    <div class="page-container">
        <div class="nav-header">
            <router-link to="/" class="nav-link" active-class="active">翻译任务</router-link>
            <router-link to="/proofread" class="nav-link" active-class="active">字幕校对</router-link>
        </div>
        <div v-loading="loading" class="proofread-editor">
        <el-page-header @back="goBack" title="返回">
            <template #content>
                <span class="text-large font-600 mr-3">字幕校对</span>
            </template>
        </el-page-header>

        <div class="editor-container">
            <div class="editor-panel">
                <h3>原始字幕 ({{ record?.original_language }})</h3>
                <el-input
                    v-model="originalSrt"
                    type="textarea"
                    :rows="25"
                    readonly
                    placeholder="原始字幕"
                />
            </div>

            <div class="editor-panel">
                <h3>翻译字幕 ({{ record?.translated_language }}) - 可编辑</h3>
                <el-input
                    v-model="translatedSrt"
                    type="textarea"
                    :rows="25"
                    placeholder="翻译字幕"
                />
            </div>
        </div>

        <div class="editor-footer">
            <el-button @click="goBack">取消</el-button>
            <el-button type="primary" @click="submitProofread" :loading="submitting">
                提交校对
            </el-button>
        </div>
    </div>
    </div>
</template>

<script lang="ts" setup>
import { ref, onBeforeMount } from "vue"
import { useRoute, useRouter } from "vue-router"
import { ElNotification, ElMessage } from 'element-plus'
import { proofreadRecord, getProofreadSrt, submitProofread as submitProofreadApi } from "../api/api.ts"

const route = useRoute()
const router = useRouter()
const loading = ref(false)
const submitting = ref(false)
const recordId = Number(route.params.id)
const record = ref<proofreadRecord | null>(null)
const originalSrt = ref("")
const translatedSrt = ref("")

onBeforeMount(() => {
    loadSrtContent()
})

function loadSrtContent() {
    loading.value = true
    getProofreadSrt(recordId).then((res: any) => {
        originalSrt.value = res.data.original_srt || ""
        translatedSrt.value = res.data.translated_srt || ""
        record.value = res.data
    }).catch((err) => {
        ElNotification({
            title: 'Error',
            message: err.message || '加载失败',
            type: 'error',
        })
    }).finally(() => {
        loading.value = false
    })
}

function goBack() {
    router.push('/proofread')
}

function submitProofread() {
    if (!translatedSrt.value.trim()) {
        ElMessage.warning('请输入校对后的字幕')
        return
    }

    submitting.value = true
    submitProofreadApi(recordId, translatedSrt.value).then(() => {
        ElMessage.success('提交成功，正在处理...')
        router.push('/proofread')
    }).catch((err) => {
        ElNotification({
            title: 'Error',
            message: err.message || '提交失败',
            type: 'error',
        })
    }).finally(() => {
        submitting.value = false
    })
}
</script>

<style scoped>
.page-container {
    width: 100%;
    height: 100%;
}
.nav-header {
    display: flex;
    gap: 20px;
    padding: 15px 20px;
    background-color: #f5f5f5;
    border-bottom: 1px solid #ddd;
}
.nav-link {
    text-decoration: none;
    color: #666;
    font-size: 16px;
    padding: 8px 16px;
    border-radius: 4px;
}
.nav-link:hover {
    background-color: #e0e0e0;
}
.nav-link.active {
    color: #409eff;
    background-color: #e6f7ff;
}
.proofread-editor {
    padding: 20px;
}

.editor-container {
    display: flex;
    gap: 20px;
    margin-top: 20px;
}

.editor-panel {
    flex: 1;
}

.editor-panel h3 {
    margin-bottom: 10px;
    color: #333;
}

.editor-footer {
    margin-top: 20px;
    text-align: right;
}
</style>
