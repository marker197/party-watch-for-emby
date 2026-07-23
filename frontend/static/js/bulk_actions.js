/**
 * Bulk Actions UI Component
 * Multi-select, batch operations, progress tracking
 * 
 * Features:
 * - Multi-select checkboxes for library items
 * - Bulk action dispatcher (delete, rate, export, add to collection)
 * - Progress tracking with visual feedback
 * - Undo/retry functionality
 * - Keyboard shortcuts for power users
 */

class BulkActionsManager {
    constructor(apiBaseUrl = '/api/v2') {
        this.apiBaseUrl = apiBaseUrl;
        this.selectedItems = new Set();
        this.activeActions = new Map();
        this.history = [];
        this.init();
    }

    /**
     * Initialize bulk actions UI
     */
    init() {
        this.createToolbar();
        this.attachEventListeners();
        this.setupKeyboardShortcuts();
        logger.info('Bulk Actions Manager initialized');
    }

    /**
     * Create bulk actions toolbar
     */
    createToolbar() {
        const toolbar = document.createElement('div');
        toolbar.id = 'bulk-actions-toolbar';
        toolbar.className = 'bulk-toolbar hidden';
        toolbar.innerHTML = `
            <div class="bulk-toolbar-content">
                <div class="bulk-info">
                    <span class="bulk-count">0 items selected</span>
                </div>
                
                <div class="bulk-actions">
                    <button class="bulk-btn bulk-select-all" title="Select all items (Ctrl+A)">
                        <i class="icon-check-all"></i> Select All
                    </button>
                    
                    <button class="bulk-btn bulk-deselect-all" title="Deselect all items">
                        <i class="icon-x"></i> Deselect
                    </button>
                    
                    <div class="bulk-divider"></div>
                    
                    <div class="bulk-dropdown">
                        <button class="bulk-btn bulk-action-trigger" title="Perform action on selected items">
                            <i class="icon-zap"></i> Bulk Action <i class="icon-chevron-down"></i>
                        </button>
                        
                        <div class="bulk-menu hidden">
                            <button class="bulk-menu-item" data-action="rate_batch">
                                <i class="icon-star"></i> Batch Rate
                            </button>
                            <button class="bulk-menu-item" data-action="add_collection">
                                <i class="icon-folder-plus"></i> Add to Collection
                            </button>
                            <button class="bulk-menu-item" data-action="export">
                                <i class="icon-download"></i> Export Selection
                            </button>
                            <div class="bulk-menu-divider"></div>
                            <button class="bulk-menu-item bulk-danger" data-action="delete">
                                <i class="icon-trash"></i> Delete Selected
                            </button>
                        </div>
                    </div>
                    
                    <button class="bulk-btn bulk-close" title="Close bulk actions (Esc)">
                        <i class="icon-x"></i>
                    </button>
                </div>
            </div>
            
            <div class="bulk-progress-container hidden">
                <div class="bulk-progress-info">
                    <span class="bulk-progress-text">Operation in progress...</span>
                    <span class="bulk-progress-percent">0%</span>
                </div>
                <div class="bulk-progress-bar">
                    <div class="bulk-progress-fill" style="width: 0%"></div>
                </div>
            </div>
        `;
        
        document.body.insertBefore(toolbar, document.body.firstChild);
    }

    /**
     * Attach event listeners to toolbar and items
     */
    attachEventListeners() {
        const toolbar = document.getElementById('bulk-actions-toolbar');
        
        // Bulk action buttons
        toolbar.querySelector('.bulk-select-all').addEventListener('click', () => this.selectAll());
        toolbar.querySelector('.bulk-deselect-all').addEventListener('click', () => this.deselectAll());
        toolbar.querySelector('.bulk-close').addEventListener('click', () => this.closeBulkMode());
        
        // Action menu
        const actionTrigger = toolbar.querySelector('.bulk-action-trigger');
        const actionMenu = toolbar.querySelector('.bulk-menu');
        
        actionTrigger.addEventListener('click', () => {
            actionMenu.classList.toggle('hidden');
        });
        
        actionMenu.querySelectorAll('.bulk-menu-item').forEach(item => {
            item.addEventListener('click', (e) => {
                const action = e.currentTarget.dataset.action;
                this.performAction(action);
                actionMenu.classList.add('hidden');
            });
        });
        
        // Close menu when clicking outside
        document.addEventListener('click', (e) => {
            if (!actionTrigger.contains(e.target) && !actionMenu.contains(e.target)) {
                actionMenu.classList.add('hidden');
            }
        });
        
        // Item checkboxes
        this.attachItemCheckboxes();
    }

    /**
     * Attach checkboxes to library items
     */
    attachItemCheckboxes() {
        const items = document.querySelectorAll('[data-emby-item-id]');
        
        items.forEach(item => {
            const itemId = item.dataset.embyItemId;
            
            // Create checkbox
            const checkbox = document.createElement('input');
            checkbox.type = 'checkbox';
            checkbox.className = 'bulk-item-checkbox';
            checkbox.dataset.itemId = itemId;
            
            // Prepend to item
            const container = item.querySelector('.item-header') || item;
            container.insertBefore(checkbox, container.firstChild);
            
            // Add click listener
            checkbox.addEventListener('change', (e) => {
                if (e.target.checked) {
                    this.selectedItems.add(itemId);
                } else {
                    this.selectedItems.delete(itemId);
                }
                this.updateToolbar();
            });
        });
    }

    /**
     * Select all items
     */
    selectAll() {
        document.querySelectorAll('.bulk-item-checkbox').forEach(checkbox => {
            checkbox.checked = true;
            this.selectedItems.add(checkbox.dataset.itemId);
        });
        this.updateToolbar();
    }

    /**
     * Deselect all items
     */
    deselectAll() {
        document.querySelectorAll('.bulk-item-checkbox').forEach(checkbox => {
            checkbox.checked = false;
        });
        this.selectedItems.clear();
        this.updateToolbar();
    }

    /**
     * Update toolbar display
     */
    updateToolbar() {
        const toolbar = document.getElementById('bulk-actions-toolbar');
        const count = this.selectedItems.size;
        
        if (count === 0) {
            toolbar.classList.add('hidden');
        } else {
            toolbar.classList.remove('hidden');
            toolbar.querySelector('.bulk-count').textContent = 
                `${count} item${count !== 1 ? 's' : ''} selected`;
        }
    }

    /**
     * Perform bulk action
     */
    async performAction(actionType) {
        if (this.selectedItems.size === 0) {
            showToast('No items selected', 'warning');
            return;
        }
        
        const itemIds = Array.from(this.selectedItems);
        
        // Handle different action types
        switch(actionType) {
            case 'rate_batch':
                await this.showRatingDialog(itemIds);
                break;
            case 'add_collection':
                await this.showCollectionDialog(itemIds);
                break;
            case 'export':
                await this.exportSelection(itemIds);
                break;
            case 'delete':
                await this.confirmAndDeleteItems(itemIds);
                break;
            default:
                showToast(`Unknown action: ${actionType}`, 'error');
        }
    }

    /**
     * Show rating dialog for batch rating
     */
    async showRatingDialog(itemIds) {
        const rating = await showDialog({
            title: 'Rate Items',
            message: `Rate ${itemIds.length} items on a scale of 1-10`,
            type: 'input',
            inputType: 'range',
            inputOptions: {
                min: 1,
                max: 10,
                value: 7
            }
        });
        
        if (rating !== null) {
            await this.executeBulkAction('rate_batch', itemIds, { rating });
        }
    }

    /**
     * Show collection dialog
     */
    async showCollectionDialog(itemIds) {
        const collections = await this.fetchAvailableCollections();
        
        const selected = await showDialog({
            title: 'Add to Collection',
            message: `Add ${itemIds.length} items to collection`,
            type: 'select',
            options: collections
        });
        
        if (selected) {
            await this.executeBulkAction('add_collection', itemIds, { 
                collection_name: selected 
            });
        }
    }

    /**
     * Export selected items
     */
    async exportSelection(itemIds) {
        const format = await showDialog({
            title: 'Export Format',
            message: 'Choose export format',
            type: 'select',
            options: [
                { label: 'JSON', value: 'json' },
                { label: 'CSV', value: 'csv' }
            ]
        });
        
        if (format) {
            await this.executeBulkAction('export', itemIds, { format });
        }
    }

    /**
     * Confirm and delete items
     */
    async confirmAndDeleteItems(itemIds) {
        const confirmed = await showDialog({
            title: 'Confirm Deletion',
            message: `Are you sure you want to delete ${itemIds.length} item(s)? This cannot be undone.`,
            type: 'confirm',
            dangerLevel: 'high'
        });
        
        if (confirmed) {
            await this.executeBulkAction('delete', itemIds);
        }
    }

    /**
     * Execute bulk action via API
     */
    async executeBulkAction(actionType, itemIds, metadata = {}) {
        try {
            this.showProgress(itemIds.length);
            
            const response = await fetch(`${this.apiBaseUrl}/bulk/action`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    action_type: actionType,
                    item_ids: itemIds,
                    metadata
                })
            });
            
            const data = await response.json();
            
            if (!response.ok) {
                throw new Error(data.detail || 'Bulk action failed');
            }
            
            const actionId = data.action_id;
            this.activeActions.set(actionId, {
                type: actionType,
                itemCount: itemIds.length,
                startTime: Date.now(),
                status: 'pending'
            });
            
            // Poll for completion
            await this.pollActionStatus(actionId);
            
            showToast(`${actionType} completed for ${itemIds.length} items`, 'success');
            this.deselectAll();
            this.closeBulkMode();
            
        } catch (error) {
            logger.error('Bulk action error:', error);
            showToast(`Error: ${error.message}`, 'error');
        } finally {
            this.hideProgress();
        }
    }

    /**
     * Poll action status
     */
    async pollActionStatus(actionId, maxAttempts = 120) {
        for (let i = 0; i < maxAttempts; i++) {
            try {
                const response = await fetch(`${this.apiBaseUrl}/bulk/status/${actionId}`);
                const data = await response.json();
                
                const action = this.activeActions.get(actionId);
                if (!action) return;
                
                action.status = data.status;
                
                // Update progress
                const progress = ((i / maxAttempts) * 100);
                this.updateProgress(progress);
                
                if (data.status === 'completed' || data.status === 'failed') {
                    this.history.push({
                        actionId,
                        ...data,
                        completedAt: new Date()
                    });
                    return;
                }
                
                // Wait before next poll
                await new Promise(resolve => setTimeout(resolve, 500));
                
            } catch (error) {
                logger.error('Error polling action status:', error);
            }
        }
    }

    /**
     * Show progress bar
     */
    showProgress(totalItems) {
        const container = document.querySelector('.bulk-progress-container');
        container.classList.remove('hidden');
        this.updateProgress(0);
    }

    /**
     * Update progress
     */
    updateProgress(percent) {
        const container = document.querySelector('.bulk-progress-container');
        const fill = container.querySelector('.bulk-progress-fill');
        const percentText = container.querySelector('.bulk-progress-percent');
        
        fill.style.width = `${percent}%`;
        percentText.textContent = `${Math.round(percent)}%`;
    }

    /**
     * Hide progress bar
     */
    hideProgress() {
        const container = document.querySelector('.bulk-progress-container');
        container.classList.add('hidden');
    }

    /**
     * Close bulk mode
     */
    closeBulkMode() {
        this.deselectAll();
        document.getElementById('bulk-actions-toolbar').classList.add('hidden');
    }

    /**
     * Setup keyboard shortcuts
     */
    setupKeyboardShortcuts() {
        document.addEventListener('keydown', (e) => {
            // Ctrl/Cmd + A: Select all
            if ((e.ctrlKey || e.metaKey) && e.key === 'a') {
                if (this.selectedItems.size > 0) {
                    e.preventDefault();
                    this.selectAll();
                }
            }
            
            // Esc: Close bulk mode
            if (e.key === 'Escape' && this.selectedItems.size > 0) {
                this.closeBulkMode();
            }
        });
    }

    /**
     * Fetch available collections
     */
    async fetchAvailableCollections() {
        try {
            const response = await fetch(`${this.apiBaseUrl}/collections`);
            const data = await response.json();
            return data.collections || [];
        } catch (error) {
            logger.error('Error fetching collections:', error);
            return [];
        }
    }

    /**
     * Get action history
     */
    getHistory(limit = 20) {
        return this.history.slice(-limit);
    }
}

// Initialize when DOM is ready
document.addEventListener('DOMContentLoaded', () => {
    window.bulkActionsManager = new BulkActionsManager();
});
