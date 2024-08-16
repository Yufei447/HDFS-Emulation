<template>
    <div>
        <div class="center">
            <div class="back">
                <div class="Button inlineBlock">
                    <el-button type="primary" @click="backToLastDirectory">back</el-button>
                </div>
                <h3 class="inlineBlock">
                    {{ curDirectory }}
                </h3>
            </div>

            <!--New Folder && New File-->
            <div class="functionButtons">
                <div class="Button inlineBlock">
                    <el-button @click="showNewFolderDialog = true">New Folder</el-button>
                </div>
                <div class="Button inlineBlock">
                    <el-button @click="showNewFileDialog = true">New File</el-button>
                </div>
            </div>

            <!--File Icon-->
            <div v-for="(item, index) in allFiles" :key="index" class="inlineBlock divFor">
                <span @click="() => enterSubDirectory(item.path, item.isFile)">
                    <FileIcon :is-file="item.isFile" :file-name="item.name"/>
                </span>
            </div>

            <!--Show file content && Delete file-->
            <el-dialog
                :title="fileName"
                :visible.sync="showFileContentOuterDialog"
                width="30%">

                <!--Delete file inner-->
                <el-dialog
                    width="30%"
                    title="Delete Confirm"
                    :visible.sync="deleteFileInnerDialog"
                    append-to-body>
                    <span>Are you sure you want to delete the file? (Operation can't reback!)</span>
                    <span slot="footer" class="dialog-footer">
                        <el-button type="primary" @click="deleteFileInnerDialog = false">Close</el-button>
                        <el-button type="danger" @click="deleteFile">Delete</el-button>
                    </span>
                </el-dialog>

                <!--File Content && Partition Content-->
                <el-dialog
                    width="60%"
                    :title="partitionNumber === '' ? fileName : fileName + ' - Partition' +partitionNumber"
                    :visible.sync="fileContentInnerDialog"
                    :before-close="closeFileContentInnerDialog"
                    append-to-body>
                    <span>{{ fileContent }}</span>
                    <span slot="footer" class="dialog-footer">
                        <el-button type="primary" @click="closeFileContentInnerDialog">Close</el-button>
                    </span>
                </el-dialog>


                <!--Partition Content Button-->
                <span v-for="(item, index) in partitionList" :key="index">
                    <el-button type="info" size="small" plain class="partition-button"
                               @click="() => seeFilePartitionContent(index + 1)">
                        Partition{{ index + 1 }}
                    </el-button>
                </span>

                <!--All File Content Button-->
                <div class="all-content-button">
                    <el-button type="info" size="small" plain @click="seeFileContent">
                        See File Content
                    </el-button>
                </div>
                
                <!-- Delete File -->
                <span slot="footer" class="dialog-footer">
                    <el-button type="primary" @click="showFileContentOuterDialog = false">Close</el-button>
                    <el-button type="danger" @click="deleteFileInnerDialog = true">Delete</el-button>
                </span>
            </el-dialog>

            
            <!--New folder-->
            <el-dialog
                :title="newFolderDialogTitle"
                :visible.sync="showNewFolderDialog"
                :before-close="cancelNewFolder"
                width="30%">
                <el-input placeholder="Please input folder name" v-model="newFolderDirectoryName"></el-input>
                <span slot="footer" class="dialog-footer">
                    <el-button @click="cancelNewFolder">Cancel</el-button>
                    <el-button type="primary" @click="newFolder">Confirm</el-button>
                </span>
            </el-dialog>

            <!--New file-->
            <el-dialog
                title="New File"
                :visible.sync="showNewFileDialog"
                :before-close="cancelNewFile"
                width="30%">
                <el-input placeholder="Please input file name" v-model="newFileName"></el-input>
                <el-upload
                    class="upload-button"
                    ref="upload"
                    action=""
                    :auto-upload="false"
                    :file-list="fileList"
                    :on-change="handleChange">
                    <el-button slot="trigger" size="small">select file</el-button>
                </el-upload>
                <span slot="footer" class="dialog-footer">
                    <el-button @click="cancelNewFile">Cancel</el-button>
                    <el-button type="primary" @click="newFile">Confirm</el-button>
                </span>
            </el-dialog>
        </div>
    </div>
</template>

<script>
import axios from "axios";
import FileIcon from "@/components/FileIcon";
import {HDFS} from "@/url";


export default {
    name: 'HomePage',
    components: {
        FileIcon
    },
    data() {
        return {
            allFiles: [],
            curDirectory: "",
            stack: [],

            // show file content
            showFileContentOuterDialog: false,
            deleteFileInnerDialog: false,
            fileContentInnerDialog: false,
            fileName: "",
            partitionList: [],
            fileContent: "",
            partitionNumber: "",

            // New Folder
            showNewFolderDialog: false,
            newFolderDialogTitle: "New Folder",
            newFolderDirectoryName: "",

            // New File
            showNewFileDialog: false,
            newFileName: "",
            newFilePartitionNumber: "",
            fileList: [],

            // db
            selectedDB: 'firebase',
        }
    },

    methods: {
        async ls(path) {
            return await axios.get(HDFS[this.selectedDB] + "/ls", {
                params: {path: path}
            })
        },

        async cat(path) {
            return await axios.get(HDFS[this.selectedDB] + "/cat", {
                params: {path: path}
            })
        },

        async mkdir(directoryName) {
            return await axios.post(HDFS[this.selectedDB] + "/mkdir", {
                'directory.name': this.curDirectory === "/" ?
                    this.curDirectory + directoryName : this.curDirectory + "/" + directoryName
            })
        },

        async rm(path) {
            return await axios.delete(HDFS[this.selectedDB] + "/rm", {
                data: {path: path}
            })
        },

        async getNewFileContent(form) {
            return await axios.post(HDFS[this.selectedDB] + '/file', form, {
            headers: {
                'Content-Type': 'multipart/form-data'
            }
            })
        },

        async put(content) {
            return await axios.post(HDFS[this.selectedDB] + "/put", {
                directory: this.curDirectory,
                filename: this.newFileName,
                partition: parseInt(this.newFilePartitionNumber),
                content: content
            })
        },

        async getPartitionLocations(path) {
            return await axios.get(HDFS[this.selectedDB] + "/getPartitionLocations", {
                params: {path: path}
            })
        },

        async readPartition(path, partition) {
            return await axios.get(HDFS[this.selectedDB] + "/readPartition", {
                params: {path: path, partition: partition}
            })
        },


        initAllFiles() {
            this.ls("/").then(res => {
                this.allFiles = res.data.data
                this.curDirectory = "/"
            })
        },

        enterSubDirectory(subdir, isFile) {
            if (isFile) {
                this.getPartitionLocations(subdir).then(res => {
                    this.showFileContentOuterDialog = true
                    this.partitionList = res.data.data
                    this.fileName = subdir
                })
                return
            }

            this.ls(subdir).then(res => {
                this.stack.push(this.curDirectory)
                this.allFiles = res.data.data
                this.curDirectory = subdir
            })
        },

        backToLastDirectory() {
            if (this.stack.length === 0) {
                return
            }
            const lastDir = this.stack.pop()
            this.ls(lastDir).then(res => {
                this.allFiles = res.data.data
                this.curDirectory = lastDir
            })
        },

        newFolder() {
            if (this.newFolderDirectoryName === "") {
                this.cancelNewFolder()
                return
            }
            this.mkdir(this.newFolderDirectoryName).then(() => {
                this.ls(this.curDirectory).then(res => {
                    this.allFiles = res.data.data
                })
            })
            this.cancelNewFolder()
        },

        cancelNewFolder() {
            this.newFolderDirectoryName = ""
            this.showNewFolderDialog = false
        },

        deleteFile() {
            this.rm(this.fileName).then(() => {
                this.ls(this.curDirectory).then(res => {
                    this.allFiles = res.data.data
                    this.deleteFileInnerDialog = false
                    this.showFileContentOuterDialog = false
                })
            })
            this.ls(this.curDirectory).then(res => {
                this.allFiles = res.data.data
            })
        },

        handleChange(file, fileList) {
            this.fileList = fileList.slice(-3);
        },

        async newFile() {
            let form = new FormData()
            if (this.fileList.length === 0) {
                return
            }
            form.append("file", this.fileList[0].raw)

            const fileContent = await this.getNewFileContent(form)
            await this.put(fileContent.data.data)

            this.ls(this.curDirectory).then(res => {
                this.allFiles = res.data.data
            })

            this.cancelNewFile()
        },

        cancelNewFile() {
            this.newFileName = ""
            this.newFilePartitionNumber = null
            this.showNewFileDialog = false
            this.fileList = []
        },

        seeFileContent() {
            this.cat(this.fileName).then(res => {
                this.fileContent = JSON.stringify(res.data.data)
                this.fileContentInnerDialog = true
            })
            
        },

        seeFilePartitionContent(partition) {
            this.readPartition(this.fileName, partition).then(res => {
                this.fileContent = res.data.data
                this.partitionNumber = partition
                this.fileContentInnerDialog = true
            })
        },

        closeFileContentInnerDialog() {
            this.fileContentInnerDialog = false
            this.fileContent = ""
            this.partitionNumber = ""
        },
    },

    created() {
        this.initAllFiles();
    },

    watch: {
        selectedDB: function (val) {
            this.selectedDB = val
            this.initAllFiles()
        }
    }
}
</script>

<style scoped>


.all-content-button {
    margin-top: 20px;
}

.partition-button {
    margin-right: 20px;
}

.upload-button {
    margin-top: 10px;
}

.functionButtons {
    margin-bottom: 25px;
}

.center {
    width: 750px;
    margin: 0 auto;
    padding-left: 100px;
}

.Button {
    margin-right: 20px;
    margin-bottom: 20px;
}

.divFor {
    margin-right: 60px;
    text-align: center;
}

.inlineBlock {
    display: inline-block;
}

.back {
    margin: 15px auto 15px;
}

h3 {
    margin: 40px 0 0;
}

ul {
    list-style-type: none;
    padding: 0;
}

li {
    display: inline-block;
    margin: 0 10px;
}

a {
    color: #42b983;
}
</style>
