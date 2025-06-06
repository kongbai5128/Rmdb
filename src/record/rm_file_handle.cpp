/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */
/*
始终维护一个空闲链，file_hdr_.first_free_page_no指向第一个空闲页面，使用page_hdr_.next_free_page_no指向下一个空闲页面。
到达页面满时，file_hdr_.first_free_page_no指向下一个空闲页面。若没有空闲页面，则file_hdr_.first_free_page_no为INVALID_PAGE_ID。
*/
#include "rm_file_handle.h"
/**
 * @description: 获取当前表中记录号为rid的记录
 * @param {Rid&} rid 记录号，指定记录的位置
 * @param {Context*} context
 * @return {unique_ptr<RmRecord>} rid对应的记录对象指针
 */
std::unique_ptr<RmRecord> RmFileHandle::get_record(const Rid& rid, Context* context) const {
    // Todo:
    // 1. 获取指定记录所在的page handle
    // 2. 初始化一个指向RmRecord的指针（赋值其内部的data和size）
    RmPageHandle page_handle = fetch_page_handle(rid.page_no);// pin页面，增加pin_count
    char *slot = page_handle.get_slot(rid.slot_no);
    auto record = std::make_unique<RmRecord>(file_hdr_.record_size,slot);
    buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), false);  // 释放页面，但不标记为脏页
    return record;
}

/**
 * @description: 在当前表中插入一条记录，不指定插入位置
 * @param {char*} buf 要插入的记录的数据
 * @param {Context*} context
 * @return {Rid} 插入的记录的记录号（位置）
 */
Rid RmFileHandle::insert_record(char* buf, Context* context) {
    // Todo:
    // 1. 获取当前未满的page handle
    // 2. 在page handle中找到空闲slot位置
    // 3. 将buf复制到空闲slot位置
    // 4. 更新page_handle.page_hdr中的数据结构
    // 注意考虑插入一条记录后页面已满的情况，需要更新file_hdr_.first_free_page_no
    RmPageHandle page_handle = create_page_handle();//  pin页面，增加pin_count
    int id = Bitmap::first_bit(false, page_handle.bitmap, file_hdr_.num_records_per_page);
    char *slot = page_handle.get_slot(id);
    memcpy(slot, buf, file_hdr_.record_size);
    Bitmap::set(page_handle.bitmap, id);
    page_handle.page_hdr->num_records++;
    if (page_handle.page_hdr->num_records == file_hdr_.num_records_per_page) {
        // 如果当前页面已满，则更新file_hdr_.first_free_page_no
        file_hdr_.first_free_page_no = page_handle.page_hdr->next_free_page_no;
    }
    Rid rid = {page_handle.page->get_page_id().page_no, id};
    buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), true);  // 释放页面并标记为脏页
    return rid;  // 返回插入记录的记录号（位置）
}

/**
 * @description: 在当前表中的指定位置插入一条记录
 * @param {Rid&} rid 要插入记录的位置
 * @param {char*} buf 要插入记录的数据
 */
void RmFileHandle::insert_record(const Rid& rid, char* buf) {
    RmPageHandle page_handle = fetch_page_handle(rid.page_no);// pin页面，增加pin_count
    char *slot = page_handle.get_slot(rid.slot_no);
    memcpy(slot, buf, file_hdr_.record_size);
    Bitmap::set(page_handle.bitmap, rid.slot_no);  // 设置bitmap中对应位置为1
    page_handle.page_hdr->num_records++;
    if (page_handle.page_hdr->num_records == file_hdr_.num_records_per_page) {
        // 如果当前页面已满，则更新file_hdr_.first_free_page_no
        file_hdr_.first_free_page_no = page_handle.page_hdr->next_free_page_no;
    }
    buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), true);  // 释放页面并标记为脏页
}

/**
 * @description: 删除记录文件中记录号为rid的记录
 * @param {Rid&} rid 要删除的记录的记录号（位置）
 * @param {Context*} context
 */
void RmFileHandle::delete_record(const Rid& rid, Context* context) {
    // Todo:
    // 1. 获取指定记录所在的page handle
    // 2. 更新page_handle.page_hdr中的数据结构
    // 注意考虑删除一条记录后页面未满的情况，需要调用release_page_handle()
    RmPageHandle page_handle = fetch_page_handle(rid.page_no);// pin页面，增加pin_count
    Bitmap::reset(page_handle.bitmap, rid.slot_no);  // 将bitmap中对应位置置为0
    page_handle.page_hdr->num_records--;
    if(page_handle.page_hdr->num_records == file_hdr_.num_records_per_page - 1) {//确保是由满变为页面未满的情况
        release_page_handle(page_handle);
    }
    buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), true);  // 释放页面并标记为脏页
}


/**
 * @description: 更新记录文件中记录号为rid的记录
 * @param {Rid&} rid 要更新的记录的记录号（位置）
 * @param {char*} buf 新记录的数据
 * @param {Context*} context
 */
void RmFileHandle::update_record(const Rid& rid, char* buf, Context* context) {
    // Todo:
    // 1. 获取指定记录所在的page handle
    // 2. 更新记录
    RmPageHandle page_handle = fetch_page_handle(rid.page_no);// pin页面，增加pin_count
    char *slot = page_handle.get_slot(rid.slot_no);
    memcpy(slot, buf, file_hdr_.record_size);  // 将buf复制到指定slot位置
    buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), true);  // 释放页面并标记为脏页
}

/**
 * 以下函数为辅助函数，仅提供参考，可以选择完成如下函数，也可以删除如下函数，在单元测试中不涉及如下函数接口的直接调用
*/
/**
 * @description: 获取指定页面的页面句柄
 * @param {int} page_no 页面号
 * @return {RmPageHandle} 指定页面的句柄
 */
RmPageHandle RmFileHandle::fetch_page_handle(int page_no) const {
    // Todo:
    // 使用缓冲池获取指定页面，并生成page_handle返回给上层
    // if page_no is invalid, throw PageNotExistError exception
    if (page_no < 0 || page_no >= file_hdr_.num_pages) {
        throw PageNotExistError("Page_no is invalid", page_no);
    }
    
    // 通过缓冲池获取页面 ,pin增加
    Page* page = buffer_pool_manager_->fetch_page({fd_, page_no});// pin页面，增加pin_count
    // 创建页面句柄
    return RmPageHandle(&file_hdr_, page);
}

/**
 * @description: 创建一个新的page handle
 * @return {RmPageHandle} 新的PageHandle
 */
RmPageHandle RmFileHandle::create_new_page_handle() {
    // Todo:
    // 1.使用缓冲池来创建一个新page
    // 2.更新page handle中的相关信息
    // 3.更新file_hdr_
    PageId new_page_id;// 创建新页的PageID（fd为当前文件，页号先不分配）
    new_page_id.fd = fd_;
    Page* new_page = buffer_pool_manager_->new_page(&new_page_id);// pin页面，增加pin_count
    // 初始化新页数据结构
    RmPageHandle page_handle(&file_hdr_, new_page);
    char* bitmap = page_handle.bitmap;
    Bitmap::init(bitmap, file_hdr_.bitmap_size);
    page_handle.page_hdr->num_records = 0;  // 初始化记录数为0
    page_handle.page_hdr->next_free_page_no = file_hdr_.first_free_page_no;  // 链接到当前空闲链表
    file_hdr_.first_free_page_no = new_page_id.page_no;  // 新页成为空闲链表头
    file_hdr_.num_pages++;
    return page_handle;
}

/**
 * @brief 创建或获取一个空闲的page handle
 *
 * @return RmPageHandle 返回生成的空闲page handle
 * @note pin the page, remember to unpin it outside!
 */
RmPageHandle RmFileHandle::create_page_handle() {
    // Todo:
    // 1. 判断file_hdr_中是否还有空闲页
    //     1.1 没有空闲页：使用缓冲池来创建一个新page；可直接调用create_new_page_handle()
    //     1.2 有空闲页：直接获取第一个空闲页
    // 2. 生成page handle并返回给上层
    if(file_hdr_.first_free_page_no == INVALID_PAGE_ID)
        return create_new_page_handle();
    else
        return fetch_page_handle(file_hdr_.first_free_page_no);
}

/**
 * @description: 当一个页面从没有空闲空间的状态变为有空闲空间状态时，更新文件头和页头中空闲页面相关的元数据
 */
void RmFileHandle::release_page_handle(RmPageHandle&page_handle) {
    // Todo:
    // 当page从已满变成未满，考虑如何更新：
    // 1. page_handle.page_hdr->next_free_page_no
    // 2. file_hdr_.first_free_page_no
    page_handle.page_hdr->next_free_page_no = file_hdr_.first_free_page_no;  // 将当前页的next_free_page_no指向file_hdr_.first_free_page_no
    file_hdr_.first_free_page_no = page_handle.page->get_page_id().page_no;  // 更新file_hdr_.first_free_page_no为当前页的页号
}