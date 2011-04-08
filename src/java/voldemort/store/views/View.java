package voldemort.store.views;

import voldemort.annotations.Experimental;
import voldemort.store.Store;

/**
 * The interface for defining a view.
 * 
 * This interface provides a translation from the view's type to the target
 * store's type and vice versa. If one direction is not supported, the
 * unimplemented method should throw an
 * {@link voldemort.store.views.UnsupportedViewOperationException}.
 * 
 * 
 * @param <K> The type of the key
 * @param <V> The type of things in the view
 * @param <S> The type of things in the store
 */
@Experimental
public interface View<K, V, S> {

    /**
     * Translate from the store type to the view type
     * 
     * @param targetStore The store behind the view
     * @param s The value for the store
     * @return The value for the view
     * @throws UnsupportedViewOperationException If this direction of
     *         translation is not allowed
     */
    public V storeToView(Store<K, S> targetStore, K k, S s)
            throws UnsupportedViewOperationException;

    /**
     * Translate from the view type to the store type
     * 
     * @param targetStore The store behind the view
     * @param k The key
     * @param v The value
     * @return The store type
     * @throws UnsupportedViewOperationException If this direction of
     *         translation is not allowed
     */
    public S viewToStore(Store<K, S> targetStore, K k, V v)
            throws UnsupportedViewOperationException;

}
